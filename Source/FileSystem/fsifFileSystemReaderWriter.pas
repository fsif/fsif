(******************************************************************************)
(*                                                                            *)
(*    Description:   File System Reader/Writer                                *)
(*    Version:       1.00                                                     *)
(*    Copyright:     Copyright (c) 2002-2018, David J Butler                  *)
(*                   All rights reserved.                                     *)
(*    License:       MIT License                                              *)
(*    Github:        https://github.com/fsif                                  *)
(*                                                                            *)
(*  Revision history:                                                         *)
(*    2002/08/09  1.00  Created cFileSystemReaderWriter from cFileSystem.     *)
(*                                                                            *)
(******************************************************************************)

{$INCLUDE fsifFileSystem.inc}

unit fsifFileSystemReaderWriter;

interface

uses
  { System }
  SyncObjs,

  { Fundamentals }
  flcStdTypes,
  flcStreams,

  { FileSystem }
  fsifFileSystemStructs;



{                                                                              }
{ Constants                                                                    }
{                                                                              }
const
  FS_MinimumCacheSize = 8;  // 16 Kb
  FS_DefaultCacheSize = 48; // 96 Kb



{                                                                              }
{ TFileSystemBlock                                                             }
{                                                                              }
type
  TFileSystemBlock = class
    Idx        : Int64;
    Data       : Pointer;
    FirstUse   : Word32;
    Hits       : Word32;
    Locks      : Integer;
    WriteLocks : Integer;
    Changed    : Boolean;
    Encrypted  : Boolean;

    constructor Create;
    destructor Destroy; override;

    procedure ClearData;
    procedure SetUsed(const Idx: Int64; const AgeCounter: Word32);
    procedure SetUnused;
    function  IsFlushPending: Boolean;
  end;



{                                                                              }
{ TFileSystemReaderWriter                                                      }
{                                                                              }
type
  TBlockAccessOptions = set of (
      baReadOnlyAccess,
      baExclusiveAccess,
      baDontRead);

  TBlockReleaseOptions = set of (
      brDataChanged,
      brNoLazyWrite,
      brDataInvalidated);

  TFileSystemReaderWriter = class
  protected
    FStream        : AStream;
    FStreamOwner   : Boolean;
    FCacheSize     : Integer;

    FLock          : TCriticalSection;
    FAgeCounter    : Word32;
    FBlockCache    : array of TFileSystemBlock;
    FEncryptionKey : RawByteString;
    FUseEncryption : Boolean;

    procedure SetCacheSize(const CacheSize: Integer);
    function  GetCacheSizeKb: Integer;
    procedure SetCacheSizeKb(const CacheSizeKb: Integer);
    function  GetStreamSize: Int64;
    procedure SetStreamSize(const StreamSize: Int64);

    function  LocateBlock(const Idx: Int64): TFileSystemBlock;
    function  AllocateBlock(const Idx: Int64): TFileSystemBlock;

  public
    constructor Create(const Stream: AStream; const StreamOwner: Boolean = True);
    destructor Destroy; override;

    property  Stream: AStream read FStream;
    property  StreamOwner: Boolean read FStreamOwner write FStreamOwner;
    property  StreamSize: Int64 read GetStreamSize write SetStreamSize;

    property  CacheSize: Integer read FCacheSize write SetCacheSize;
    property  CacheSizeKb: Integer read GetCacheSizeKb write SetCacheSizeKb;

    procedure Lock;
    procedure Unlock;

    function  InitEncryption(const Password: RawByteString;
              const Encryption: TFSEncryptionType): Boolean;
    procedure SetEncryption(const UseEncryption: Boolean);

    procedure StreamReadBlock(const Idx: Int64; out Buffer;
              const Size: Integer);
    procedure StreamWriteBlock(const Idx: Int64; const Buffer;
              const Size: Integer; const Encrypted: Boolean);

    function  GetBlock(const Idx: Int64; const Encrypted: Boolean;
              const AccessOptions: TBlockAccessOptions = []): TFileSystemBlock;
    procedure ReleaseBlock(const Block: TFileSystemBlock;
              const AccessOptions: TBlockAccessOptions = [];
              const ReleaseOptions: TBlockReleaseOptions = []);

    procedure Flush;
    procedure RemoveCached(const Idx: Int64);

    // Block
    procedure ReadBlock(const Idx: Int64; out Data;
              const Offset, Size: Integer);
    procedure WriteBlock(const Idx: Int64; const Data;
              const Offset, Size: Integer);

    // Header block
    procedure ReadHeaderBlock(out Header: TFileSystemHeaderBlock);
    procedure WriteHeaderBlock(const Header: TFileSystemHeaderBlock);

    // Directory block
    function  GetDirectoryBlock(const Idx: Int64;
              out DirectoryBlock: PDirectoryBlock;
              const AccessOptions: TBlockAccessOptions = []): TFileSystemBlock;
    procedure ReleaseDirectoryBlock(const Block: TFileSystemBlock;
              const AccessOptions: TBlockAccessOptions = [];
              const ReleaseOptions: TBlockReleaseOptions = []);

    // Free block
    function  GetFreeBlock(const Idx: Int64;
              out FreeBlock: PFreeBlock;
              const AccessOptions: TBlockAccessOptions = []): TFileSystemBlock;
    procedure ReleaseFreeBlock(const Block: TFileSystemBlock;
              const AccessOptions: TBlockAccessOptions = [];
              const ReleaseOptions: TBlockReleaseOptions = []);

    // Allocation block
    function  GetAllocationBlock(const Idx: Int64;
              out AllocationBlock: PAllocationBlock;
              const AccessOptions: TBlockAccessOptions = []): TFileSystemBlock;
    procedure ReleaseAllocationBlock(const Block: TFileSystemBlock;
              const AccessOptions: TBlockAccessOptions = [];
              const ReleaseOptions: TBlockReleaseOptions = []);
  end;



implementation

uses
  { System }
  SysUtils;



{                                                                              }
{ TFileSystemBlock                                                             }
{                                                                              }
constructor TFileSystemBlock.Create;
begin
  inherited Create;
  Idx := FS_InvalidBlock;
end;

destructor TFileSystemBlock.Destroy;
begin
  ClearData;
  inherited Destroy;
end;

procedure TFileSystemBlock.ClearData;
var D : Pointer;
begin
  D := Data;
  if Assigned(D) then
    begin
      Data := nil;
      FillChar(D^, FS_BlockSize, #$FF);
      FreeMem(D);
    end;
end;

procedure TFileSystemBlock.SetUsed(const Idx: Int64; const AgeCounter: Word32);
begin
  self.Idx := Idx;
  FirstUse := AgeCounter;
  Hits := 1;
  if not Assigned(Data) then
    GetMem(Data, FS_BlockSize);
  Locks := 0;
  WriteLocks := 0;
  Changed := False;
end;

procedure TFileSystemBlock.SetUnused;
begin
  Idx := FS_InvalidBlock;
  Hits := 0;
  Locks := 0;
  WriteLocks := 0;
  Changed := False;
end;

function TFileSystemBlock.IsFlushPending: Boolean;
begin
  Result := (Idx <> FS_InvalidBlock) and Changed;
end;




{                                                                              }
{ TFileSystemReaderWriter                                                      }
{                                                                              }
constructor TFileSystemReaderWriter.Create(const Stream: AStream;
    const StreamOwner: Boolean);
begin
  inherited Create;
  if not Assigned(Stream) then
    raise EFileSystem.Create(FSE_InvalidParamStream);
  FStream := Stream;
  FStreamOwner := StreamOwner;
  FLock := TCriticalSection.Create;
  SetCacheSize(FS_DefaultCacheSize);
end;

destructor TFileSystemReaderWriter.Destroy;
var I : Integer;
begin
  for I := FCacheSize - 1 downto 0 do
    FreeAndNil(FBlockCache[I]);
  FBlockCache := nil;
  if FStreamOwner then
    FreeAndNil(FStream) else
    FStream := nil;
  FreeAndNil(FLock);
  //FreeAndNil(FCipher);
  inherited Destroy;
end;



{ Lock                                                                         }
procedure TFileSystemReaderWriter.Lock;
begin
  FLock.Enter;
end;

procedure TFileSystemReaderWriter.Unlock;
begin
  FLock.Leave;
end;

function TFileSystemReaderWriter.InitEncryption(const Password: RawByteString;
    const Encryption: TFSEncryptionType): Boolean;
begin
  if (Password = '') or (Encryption in [fseNone, fsePasswordOnly]) then
    begin
      // FreeAndNil(FCipher);
      Result := False;
    end else
  if Encryption <> fseCast256 then
    raise EFileSystem.Create(FSE_EncryptionTypeNotSupported)
  else
    begin
      FEncryptionKey := Password;
      Result := True;
    end;
end;

procedure TFileSystemReaderWriter.SetEncryption(const UseEncryption: Boolean);
begin
  FUseEncryption := UseEncryption;
end;



{ Cache                                                                        }
procedure TFileSystemReaderWriter.SetCacheSize(const CacheSize: Integer);
var I : Integer;
begin
  if CacheSize < FS_MinimumCacheSize then
    raise EFileSystem.Create(FSE_InvalidParamCacheSize);
  Lock;
  try
    if CacheSize = FCacheSize then
      exit;
    if CacheSize < FCacheSize then
      begin
        for I := Length(FBlockCache) - 1 downto CacheSize do
          FreeAndNil(FBlockCache[I]);
        SetLength(FBlockCache, CacheSize);
      end else
      begin
        SetLength(FBlockCache, CacheSize);
        for I := FCacheSize to CacheSize - 1 do
          FBlockCache[I] := TFileSystemBlock.Create;
      end;
    FCacheSize := CacheSize;
  finally
    Unlock;
  end;
end;

function TFileSystemReaderWriter.GetCacheSizeKb: Integer;
begin
  Result := (FCacheSize * FS_BlockSize) div 1024;
end;

procedure TFileSystemReaderWriter.SetCacheSizeKb(const CacheSizeKb: Integer);
begin
  SetCacheSize((Int64(CacheSizeKb) * 1024) div FS_BlockSize);
end;

function TFileSystemReaderWriter.GetStreamSize: Int64;
begin
  Result := FStream.Size;
end;

procedure TFileSystemReaderWriter.SetStreamSize(const StreamSize: Int64);
begin
  Lock;
  try
    FStream.Size := StreamSize;
  finally
    Unlock;
  end;
end;



{ Stream access                                                                }
procedure TFileSystemReaderWriter.StreamReadBlock(const Idx: Int64;
    out Buffer; const Size: Integer);
begin
  if (Idx < 0) or (Idx >= FS_MaximumBlocks) then
    raise EFileSystem.Create(FSE_InvalidParamBlockIndex);
  Lock;
  try
    Assert(Assigned(FStream), 'Assigned(FStream)');
    FStream.Position := Idx * FS_BlockSize;
    FStream.ReadBuffer(Buffer, Size);
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.StreamWriteBlock(const Idx: Int64;
    const Buffer; const Size: Integer; const Encrypted: Boolean);
var EncBuf: Array[0..FS_BlockSize - 1] of Byte;
begin
  if (Idx < 0) or (Idx >= FS_MaximumBlocks) then
    raise EFileSystem.Create(FSE_InvalidParamBlockIndex);
  Lock;
  try
    Assert(Assigned(FStream), 'Assigned(FStream)');
    FStream.Position := Idx * FS_BlockSize;
    if Encrypted then
      begin
        Assert(Size <= FS_BlockSize, 'Size <= FS_BlockSize');
        //Encrypt(ctTripleDES3EDE, cmCBC, cpPadPKCS5, 192,
        //  Pointer(FEncryptionKey), Length(FEncryptionKey),
        //  @Buffer, Size, @EncBuf[0], SizeOf(EncBuf),
        //  nil, 0);
        FStream.WriteBuffer(EncBuf, Size);
      end
    else
      FStream.WriteBuffer(Buffer, Size);
  finally
    Unlock;
  end;
end;



{ Entries                                                                      }
function TFileSystemReaderWriter.LocateBlock(const Idx: Int64): TFileSystemBlock;
var I : Integer;
    P : PPointer;
    E : TFileSystemBlock;
begin
  Lock;
  try
    Inc(FAgeCounter);
    P := Pointer(FBlockCache);
    for I := 0 to Length(FBlockCache) - 1 do
      begin
        E := TFileSystemBlock(P^);
        if E.Idx = Idx then
          begin
            Inc(E.Hits);
            Result := E;
            exit;
          end;
        Inc(P);
      end;
    Result := nil;
  finally
    Unlock;
  end;
end;

function TFileSystemReaderWriter.AllocateBlock(const Idx: Int64): TFileSystemBlock;
var I    : Integer;
    P, L : TFileSystemBlock;
    V, W : Word32;
begin
  Lock;
  try
    Inc(FAgeCounter);

    // Find unused or least frequently used entry
    Result := nil;
    L := nil;
    V := 0;
    for I := 0 to Length(FBlockCache) - 1 do
      begin
        P := FBlockCache[I];
        if P.Idx = FS_InvalidBlock then
          begin
            Result := P;
            break;
          end
        else
        if P.Locks = 0 then
          begin
            W := (FAgeCounter - P.FirstUse) div P.Hits;
            if not Assigned(L) or (W > V) then
              begin
                L := P;
                V := W;
              end;
          end;
      end;

    // Allocate
    if not Assigned(Result) then
      if not Assigned(L) then
        exit
      else
        begin
          Assert(L.WriteLocks = 0, 'L.WriteLocks = 0');
          if L.IsFlushPending then
            StreamWriteBlock(L.Idx, L.Data^, FS_BlockSize, L.Encrypted);
          Result := L;
        end;
    Result.SetUsed(Idx, FAgeCounter);
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.RemoveCached(const Idx: Int64);
var B: TFileSystemBlock;
begin
  B := LocateBlock(Idx);
  if Assigned(B) then
    B.SetUnused;
end;



{ Block access                                                                 }
function TFileSystemReaderWriter.GetBlock(const Idx: Int64;
    const Encrypted: Boolean;
    const AccessOptions: TBlockAccessOptions): TFileSystemBlock;
var B : TFileSystemBlock;
    ReadOnlyAccess : Boolean;
begin
  if Idx <= FS_InvalidBlock then
    raise EFileSystem.Create(FSE_InvalidParamBlockIndex);
  ReadOnlyAccess := baReadOnlyAccess in AccessOptions;
  Lock;
  try
    B := LocateBlock(Idx);
    if Assigned(B) then
      begin
        if not ReadOnlyAccess and (B.WriteLocks > 0) then
          raise EFileSystem.Create(FSE_BlockLocked);
        if (baExclusiveAccess in AccessOptions) and (B.Locks > 0) then
          raise EFileSystem.Create(FSE_BlockLocked);
      end
    else
      begin
        B := AllocateBlock(Idx);
        if not Assigned(B) then
          raise EFileSystem.Create(FSE_OutOfBufferSpace);
        Assert(Assigned(B.Data), 'Assigned(B.Data)');
        StreamReadBlock(Idx, B.Data^, FS_BlockSize);
        //if Encrypted then
        //  FCipher.DecryptCBC(B.Data^, B.Data^, FS_BlockSize);
      end;
    B.Encrypted := Encrypted;
    Inc(B.Locks);
    if not ReadOnlyAccess then
      Inc(B.WriteLocks);
  finally
    Unlock;
  end;
  Result := B;
end;

procedure TFileSystemReaderWriter.ReleaseBlock(const Block: TFileSystemBlock;
    const AccessOptions: TBlockAccessOptions;
    const ReleaseOptions: TBlockReleaseOptions);
begin
  if not Assigned(Block) then
    raise EFileSystem.Create(FSE_InvalidParamBuffer);
  Lock;
  try
    Assert(Block.Idx <> FS_InvalidBlock, 'Block.Idx <> FS_InvalidBlock');
    Assert(Block.Locks > 0, 'Block.Locks > 0');
    // Write changes
    if [brDataChanged, brDataInvalidated] * ReleaseOptions = [brDataChanged] then
      if brNoLazyWrite in ReleaseOptions then
        begin
          Assert(Assigned(Block.Data), 'Assigned(Block.Data)');
          StreamWriteBlock(Block.Idx, Block.Data^, FS_BlockSize, Block.Encrypted);
          Block.Changed := False;
        end
      else
        Block.Changed := True;
    // Release locks
    if not (baReadOnlyAccess in AccessOptions) then
      Dec(Block.WriteLocks);
    Dec(Block.Locks);
    // Invalidated
    if brDataInvalidated in ReleaseOptions then
      if Block.Locks = 0 then
        Block.SetUnused // Invalidate cached entry if not locked
      else
        StreamReadBlock(Block.Idx, Block.Data^, FS_BlockSize); // Refresh cached entry if locked
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.ReadBlock(const Idx: Int64; out Data;
    const Offset, Size: Integer);
var B : TFileSystemBlock;
    L : Integer;
    P : PByte;
begin
  if Size < 0 then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  if Size = 0 then
    exit;
  L := FS_BlockSize - Offset;
  if Size > L then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  Lock;
  try
    B := GetBlock(Idx, FUseEncryption, [baReadOnlyAccess]);
    try
      Assert(Assigned(B), 'Assigned(B)');
      Assert(Assigned(B.Data), 'Assigned(B.Data)');
      P := B.Data;
      Inc(P, Offset);
      Move(P^, Data, Size);
    finally
      ReleaseBlock(B, [baReadOnlyAccess], []);
    end;
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.WriteBlock(const Idx: Int64; const Data;
    const Offset, Size: Integer);
var B : TFileSystemBlock;
    L : Integer;
    P : PByte;
begin
  if Size < 0 then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  if Size = 0 then
    exit;
  L := FS_BlockSize - Offset;
  if Size > L then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  Lock;
  try
    B := GetBlock(Idx, FUseEncryption, []);
    try
      Assert(Assigned(B), 'Assigned(B)');
      Assert(Assigned(B.Data), 'Assigned(B.Data)');
      P := B.Data;
      Inc(P, Offset);
      Move(Data, P^, Size);
    except
      ReleaseBlock(B, [], [brDataInvalidated]);
      raise;
    end;
    ReleaseBlock(B, [baDontRead, baExclusiveAccess], [brDataChanged, brNoLazyWrite]);
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.Flush;
var I : Integer;
begin
  Lock;
  try
    for I := 0 to Length(FBlockCache) - 1 do
      With FBlockCache[I] do
        if IsFlushPending and (WriteLocks = 0) then
          begin
            StreamWriteBlock(Idx, Data^, FS_BlockSize, Encrypted);
            Changed := False;
          end;
  finally
    Unlock;
  end;
end;



{ Header                                                                       }
procedure TFileSystemReaderWriter.ReadHeaderBlock(out Header: TFileSystemHeaderBlock);
begin
  Lock;
  try
    StreamReadBlock(0, Header, Sizeof(TFileSystemHeaderBlock));
    FS_VerifyValidFileSystemHeaderBlock(Header);
  finally
    Unlock;
  end;
end;

procedure TFileSystemReaderWriter.WriteHeaderBlock(const Header: TFileSystemHeaderBlock);
begin
  FS_VerifyValidFileSystemHeaderBlock(Header);
  Lock;
  try
    StreamWriteBlock(0, Header, Sizeof(TFileSystemHeaderBlock), False);
  finally
    Unlock;
  end;
end;



{ Directory                                                                    }
function TFileSystemReaderWriter.GetDirectoryBlock(const Idx: Int64;
    out DirectoryBlock: PDirectoryBlock; const AccessOptions: TBlockAccessOptions): TFileSystemBlock;
begin
  Result := GetBlock(Idx, False, AccessOptions);
  DirectoryBlock := PDirectoryBlock(Result.Data);
  FS_VerifyValidDirectoryBlock(DirectoryBlock^);
end;

procedure TFileSystemReaderWriter.ReleaseDirectoryBlock(const Block: TFileSystemBlock;
    const AccessOptions: TBlockAccessOptions; const ReleaseOptions: TBlockReleaseOptions);
begin
  if [brDataInvalidated, brDataChanged] * ReleaseOptions = [brDataChanged] then
    FS_VerifyValidDirectoryBlock(PDirectoryBlock(Block.Data)^);
  ReleaseBlock(Block, AccessOptions, ReleaseOptions);
end;



{ Free block                                                                   }
function TFileSystemReaderWriter.GetFreeBlock(const Idx: Int64;
    out FreeBlock: PFreeBlock; const AccessOptions: TBlockAccessOptions): TFileSystemBlock;
begin
  Result := GetBlock(Idx, False, AccessOptions);
  FreeBlock := PFreeBlock(Result.Data);
  FS_VerifyValidFreeblock(FreeBlock^);
end;

procedure TFileSystemReaderWriter.ReleaseFreeBlock(const Block: TFileSystemBlock;
    const AccessOptions: TBlockAccessOptions; const ReleaseOptions: TBlockReleaseOptions);
begin
  if [brDataInvalidated, brDataChanged] * ReleaseOptions = [brDataChanged] then
    FS_VerifyValidFreeblock(PFreeBlock(Block.Data)^);
  ReleaseBlock(Block, AccessOptions, ReleaseOptions);
end;



{ Allocation block                                                             }
function TFileSystemReaderWriter.GetAllocationBlock(const Idx: Int64;
    out AllocationBlock: PAllocationBlock; const AccessOptions: TBlockAccessOptions): TFileSystemBlock;
begin
  Result := GetBlock(Idx, False, AccessOptions);
  AllocationBlock := PAllocationBlock(Result.Data);
  FS_VerifyValidAllocationBlock(AllocationBlock^);
end;

procedure TFileSystemReaderWriter.ReleaseAllocationBlock(const Block: TFileSystemBlock;
    const AccessOptions: TBlockAccessOptions; const ReleaseOptions: TBlockReleaseOptions);
begin
  if [brDataInvalidated, brDataChanged] * ReleaseOptions = [brDataChanged] then
    FS_VerifyValidAllocationBlock(PAllocationBlock(Block.Data)^);
  ReleaseBlock(Block, AccessOptions, ReleaseOptions);
end;



end.


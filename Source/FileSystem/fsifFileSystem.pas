(******************************************************************************)
(*                                                                            *)
(*    Description:   File System in a file                                    *)
(*    Version:       1.09                                                     *)
(*    Copyright:     Copyright (c) 2002-2018, David J Butler                  *)
(*                   All rights reserved.                                     *)
(*    License:       MIT License                                              *)
(*    Github:        https://github.com/fsif                                  *)
(*                                                                            *)
(*  Revision history:                                                         *)
(*    2002/08/05  1.00  Initial version                                       *)
(*    2002/08/09  1.01  Split cFileSystemStructs and cFileSystemReaderWriter  *)
(*                      units from cFileSystem unit.                          *)
(*    2002/08/11  1.02  Added GUIDs.                                          *)
(*    2002/08/14  1.03  Added ContentType.                                    *)
(*    2002/08/16  1.04  Debugged using cFileSystemSelfTest unit.              *)
(*    2002/09/10  1.05  Additional events.                                    *)
(*    2003/01/19  1.06  Added file system encryption.                         *)
(*    2003/09/01  1.07  Minor optimizations.                                  *)
(*    2016/04/13  1.08  String changes.                                       *)
(*    2018/08/14  1.09  LongWord changes.                                     *)
(*                                                                            *)
(******************************************************************************)

{$INCLUDE fsifFileSystem.inc}

{$TYPEINFO ON}

unit fsifFileSystem;

interface

uses
  { System }
  Classes,
  SyncObjs,

  { Fundamentals }
  flcStdTypes,
  flcStreams,
  flcHash,

  { FileSystem }
  fsifFileSystemStructs,
  fsifFileSystemReaderWriter;



{                                                                              }
{ TFileSystem                                                                  }
{                                                                              }
type
  { Internal data structures                                                   }
  TFSEntryInfo = record
    Directory : Int64;
    DirBlock  : Int64;
    DirEntry  : Word;
    Entry     : TDirectoryEntry;
  end;

  TFSEntrySpec = record
    DirBlock  : Int64;
    DirEntry  : Word;
    Name      : RawByteString;
    Mask      : RawByteString;
    AttrMask  : TDirectoryEntryFlags;
    ReqAttr   : TDirectoryEntryFlags;
  end;

  TFSAllocationInfo = record
    AllocBlock : Int64;
    AllocEntry : Word;
    DataBlock  : Word;
    Offset     : Word32;
  end;
  PFSAllocationInfo = ^TFSAllocationInfo;

  TFSFreeInfo = record
    FreeBlock : Int64;
    FreeEntry : Word;
  end;

  { TFSIterator                                                                }
  {   Directory iterator                                                       }
  TFileSystem = class;
  TFSIterator = class
  protected
    FFileSystem : TFileSystem;
    FSpec       : TFSEntrySpec;
    FEntry      : TFSEntryInfo;

    procedure CheckValidEntry;

    function  GetName : RawByteString;
    function  GetSize : Int64;
    function  GetFlags : TDirectoryEntryFlags;
    function  GetCreateTime : TDateTime;
    function  GetAccessTime : TDateTime;
    function  GetModifyTime : TDateTime;

  public
    constructor Create(const FileSystem: TFileSystem; const Spec: TFSEntrySpec);

    procedure FindFirst;
    procedure FindNext;
    function  EOF: Boolean;

    property  Name: RawByteString read GetName;
    property  Size: Int64 read GetSize;
    property  Flags: TDirectoryEntryFlags read GetFlags;
    property  CreateTime: TDateTime read GetCreateTime;
    property  AccessTime: TDateTime read GetAccessTime;
    property  ModifyTime: TDateTime read GetModifyTime;
    procedure GetContentType(var ContentType: TFSContentType);
  end;

  { TFSOpenFile                                                                }
  {   Handle to an open file system file                                       }
  TFSOpenFile = class(AStream)
  protected
    FFileSystem : TFileSystem;
    FInfo       : TFSEntryInfo;
    FGUID       : FS_GUID;
    FPosition   : Int64;
    FReader     : AReaderEx;
    FWriter     : AWriterEx;

    procedure LockFileSystem;
    procedure UnlockFileSystem;
    procedure ReadDirectoryEntry;
    procedure WriteDirectoryEntry(const MayCache: Boolean);

    function  GetReader: AReaderEx; override;
    function  GetWriter: AWriterEx; override;
    function  GetSize: Int64; override;
    procedure SetSize(const Size: Int64); override;
    function  GetPosition: Int64; override;
    procedure SetPosition(const Position: Int64); override;
    function  GetGUIDStr: RawByteString;
    function  GetGUID(const Idx: Integer): Word32;

  public
    constructor Create(const FileSystem: TFileSystem; const Info: TFSEntryInfo);
    destructor Destroy; override;

    property  GUIDStr: RawByteString read GetGUIDStr;
    property  GUID[const Idx: Integer]: Word32 read GetGUID;
    procedure GetContentType(var ContentType: TFSContentType);
    procedure SetContentType(const ContentType: TFSContentType);

    function  Read(var Buffer; const Size: Integer): Integer; override;
    function  Write(const Buffer; const Size: Integer): Integer; override;
  end;

  { TFSFileStream                                                              }
  {   TStream wrapper for an open file system file.                            }
  TFSFileStream = class(TStream)
  protected
    FOpenFile : TFSOpenFile;

    procedure SetSize(NewSize: Longint); overload; override;
    {$IFNDEF DELPHI5_DOWN}
    procedure SetSize(const NewSize: Int64); overload; override;
    {$ENDIF}

  public
    constructor Create(const OpenFile: TFSOpenFile);

    property  OpenFile: TFSOpenFile read FOpenFile;
    function  Read(var Buffer; Count: Longint): Longint; override;
    function  Write(const Buffer; Count: Longint): Longint; override;
    function  Seek(Offset: Longint; Origin: Word): Longint; overload; override;
    {$IFNDEF DELPHI5_DOWN}
    function  Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; overload; override;
    {$ENDIF}
  end;

  { TFileSystem                                                                }
  {   File system implementation using a stream.                               }
  TFSFileOpenMode = (
      fomOpenReadWrite,
      fomOpenReadOnly,
      fomCreate,
      fomCreateIfNotExist);
  TFileSystemOptions = Set of (
      fsoAutoGrow,
      fsoDontUpdateAccessTime);
  TFileSystemEvent = procedure (FileSystem: TFileSystem) of object;
  TFileSystemWarningEvent = procedure(FileSystem: TFileSystem;
      WarningCode: Integer) of object;
  TFileSystemNameEvent = procedure (FileSystem: TFileSystem;
      Name: RawByteString) of object;
  TFileSystemNameConfirmEvent = procedure (FileSystem: TFileSystem;
      Name: RawByteString; var Allow: Boolean) of object;
  TFileSystemGrowEvent = procedure (FileSystem: TFileSystem;
      OldSize: Int64; var NewSize: Int64) of object;
  TFileSystemGetPasswordEvent = procedure (FileSystem: TFileSystem;
      var Password: RawByteString) of object;
  TFileSystem = class(TComponent)
  protected
    FOptions                 : TFileSystemOptions;
    FPathSeparator           : AnsiChar;
    FMaxSize                 : Int64;
    FOpen                    : Boolean;
    FOnWarning               : TFileSystemWarningEvent;
    FOnGetPassword           : TFileSystemGetPasswordEvent;
    FOnBeforeOpen            : TFileSystemEvent;
    FOnBeforeClose           : TFileSystemEvent;
    FOnOpen                  : TFileSystemEvent;
    FOnClose                 : TFileSystemEvent;
    FOnGrow                  : TFileSystemGrowEvent;
    FOnFormatComplete        : TFileSystemEvent;
    FOnBeforeFileDelete      : TFileSystemNameConfirmEvent;
    FOnFileDelete            : TFileSystemNameEvent;
    FOnFileOpen              : TFileSystemNameEvent;
    FOnBeforeDirectoryRemove : TFileSystemNameConfirmEvent;
    FOnDirectoryMake         : TFileSystemNameEvent;
    FOnDirectoryRemove       : TFileSystemNameEvent;

    FLock          : TCriticalSection;
    FStream        : TFileSystemReaderWriter;
    FHeader        : TFileSystemHeaderBlock;
    FDeferredOpen  : Boolean;
    FPasswordHash  : T128BitDigest;
    FUseEncryption : Boolean;

    procedure Init; virtual;
    procedure LogWarning(const WarningCode: Integer); virtual;

    procedure Lock;
    procedure Unlock;

    procedure SetOpen(const Open: Boolean);
    procedure SetOptions(const Options: TFileSystemOptions);
    procedure SetPathSeparator(const PathSeparator: AnsiChar);

    procedure Loaded; override;

    function  InitEncryption(const Password: RawByteString;
              const Encryption: TFSEncryptionType): Boolean;

    procedure InitHeader;
    procedure ReadHeader;
    procedure WriteHeader;

    function  GetGUID(const Idx: Integer): Word32;
    function  GetGUIDStr: RawByteString;
    function  GetFileSystemName: RawByteString;
    procedure SetFileSystemName(const Name: RawByteString);
    function  GetUserData: RawByteString;
    procedure SetUserData(const UserData: RawByteString);

    function  FindFreeBlock(var Info: TFSFreeInfo): Int64;
    function  ReleaseFreeBlock: Int64;
    function  RequireFreeBlock: Int64;
    function  FindAvailFreeEntry(var Info: TFSFreeInfo): Boolean;
    procedure AddFreeBlock(const Block: Int64);

    function  FindDirectoryEntry(const Spec: TFSEntrySpec;
              out Entry: TFSEntryInfo): Boolean;
    function  FindUnusedDirectoryEntry(const Directory: Int64;
              out Entry: TFSEntryInfo): Boolean;
    function  FindDirectoryEntryByName(const Directory: Int64;
              const Name: RawByteString; var Entry: TFSEntryInfo): Boolean;

    procedure DeleteDirectoryEntry(const Entry: TFSEntryInfo);
    procedure RenameDirectoryEntry(const Entry: TFSEntryInfo; const Name: RawByteString);
    procedure AddDirectoryEntry(const Directory: Int64;
              const Entry: TDirectoryEntry; var Info: TFSEntryInfo);
    procedure WriteDirectoryEntry(const Entry: TFSEntryInfo);

    function  FindRelDirectory(const Directory: Int64; const RelPath: RawByteString): Int64;
    function  FindDirectory(const Directory: RawByteString): Int64;
    function  FindFile(const FileName: RawByteString; var Info: TFSEntryInfo): Boolean;
    function  FindDirectoryInfo(const Directory: RawByteString; var Info: TFSEntryInfo): Boolean;

    procedure GetAllocEntry(var Info: TFSAllocationInfo);
    procedure FreeAllocEntry(const Info: TFSAllocationInfo);
    procedure ResizeAllocEntry(var Info: TFSAllocationInfo; const Size: Integer);

    procedure GetAllocInfo(const FileInfo: TFSEntryInfo;
              const Offset: Int64; var AllocInfo: TFSAllocationInfo);
    procedure GrowAlloc(var FileInfo: TFSEntryInfo;
              var AllocInfo: TFSAllocationInfo; const Size: Int64);
    procedure ShrinkAlloc(var FileInfo: TFSEntryInfo;
              var AllocInfo: TFSAllocationInfo; const Size: Int64);
    procedure ResizeAlloc(var FileInfo: TFSEntryInfo; const Size: Int64);

    function  GetSize: Int64;
    procedure SetSize(const Size: Int64);
    procedure Grow;

    procedure CheckOpen;
    procedure CheckNotOpen;

  public
    constructor Create(AOwner: TComponent); override;
    constructor CreateEx(const Stream: AStream;
                const StreamOwner: Boolean = True);
    destructor Destroy; override;

    property  Stream: TFileSystemReaderWriter read FStream;
    property  Options: TFileSystemOptions read FOptions write SetOptions
              default [fsoAutoGrow];
    property  PathSeparator: AnsiChar read FPathSeparator write SetPathSeparator
              default FS_DefaultPathSeparator;
    property  MaxSize: Int64 read FMaxSize write FMaxSize default 0;

    property  OnWarning: TFileSystemWarningEvent read FOnWarning write FOnWarning;

    procedure Format(
              const Password: RawByteString = '';
              const Encryption: TFSEncryptionType = fseNone); virtual;
    procedure Open(const Password: RawByteString = ''); virtual;
    procedure Close; virtual;
    property  IsOpen: Boolean read FOpen write SetOpen default False;

    property  OnGetPassword: TFileSystemGetPasswordEvent read FOnGetPassword write FOnGetPassword;
    property  OnBeforeOpen: TFileSystemEvent read FOnBeforeOpen write FOnBeforeOpen;
    property  OnOpen: TFileSystemEvent read FOnOpen write FOnOpen;
    property  OnBeforeClose: TFileSystemEvent read FOnBeforeClose write FOnBeforeClose;
    property  OnClose: TFileSystemEvent read FOnClose write FOnClose;
    property  OnGrow: TFileSystemGrowEvent read FOnGrow write FOnGrow;
    property  OnFormatComplete: TFileSystemEvent read FOnFormatComplete write FOnFormatComplete;

    property  GUID[const Idx: Integer]: Word32 read GetGUID;
    property  GUIDStr: RawByteString read GetGUIDStr;
    property  FileSystemName: RawByteString read GetFileSystemName write SetFileSystemName;
    property  UserData: RawByteString read GetUserData write SetUserData;

    property  Size: Int64 read GetSize write SetSize;

    function  FileExists(const FileName: RawByteString): Boolean;
    function  GetFileFlags(const FileName: RawByteString): TDirectoryEntryFlags;
    procedure SetFileFlags(const FileName: RawByteString; const Flags: TDirectoryEntryFlags);
    function  GetFileSize(const FileName: RawByteString): Int64;
    function  GetFileCreateTime(const FileName: RawByteString): TDateTime;
    function  GetFileAccessTime(const FileName: RawByteString): TDateTime;
    function  GetFileModifyTime(const FileName: RawByteString): TDateTime;
    function  GetFileGUID(const FileName: RawByteString): FS_GUID;
    function  GetFileGUIDStr(const FileName: RawByteString): RawByteString;
    procedure GetFileContentType(const FileName: RawByteString;
              var ContentType: TFSContentType);
    procedure SetFileContentType(const FileName: RawByteString;
              const ContentType: TFSContentType);

    function  FindFirst(const Path, Mask: RawByteString;
              const AttrMask: TDirectoryEntryFlags = [];
              const ReqAttr: TDirectoryEntryFlags = []): TFSIterator;

    function  OpenFile(const FileName: RawByteString;
              const OpenMode: TFSFileOpenMode): TFSOpenFile;
    function  OpenFileStream(const FileName: RawByteString;
              const OpenMode: TFSFileOpenMode): TFSFileStream;
    procedure DeleteFile(const FileName: RawByteString);
    procedure RenameFile(const FileName, Name: RawByteString);

    procedure MakeDirectory(const Directory: RawByteString);
    procedure RemoveDirectory(const Directory: RawByteString);
    function  DirectoryExists(const Directory: RawByteString): Boolean;
    procedure RenameDirectory(const Directory, Name: RawByteString);
    procedure EnsurePathExists(const Directory: RawByteString);

    property  OnBeforeFileDelete: TFileSystemNameConfirmEvent read FOnBeforeFileDelete write FOnBeforeFileDelete;
    property  OnFileDelete: TFileSystemNameEvent read FOnFileDelete write FOnFileDelete;
    property  OnFileOpen: TFileSystemNameEvent read FOnFileOpen write FOnFileOpen;
    property  OnDirectoryMake: TFileSystemNameEvent read FOnDirectoryMake write FOnDirectoryMake;
    property  OnBeforeDirectoryRemove: TFileSystemNameConfirmEvent read FOnBeforeDirectoryRemove write FOnBeforeDirectoryRemove;
    property  OnDirectoryRemove: TFileSystemNameEvent read FOnDirectoryRemove write FOnDirectoryRemove;
  end;



{                                                                              }
{ TFileFileSystem                                                              }
{   File system implementation using a file.                                   }
{                                                                              }
const
  FS_MinimumFileSizeKb        = (FS_MinimumSize + 1023) div 1024;
  FS_DefaultInitialFileSizeKb = FS_MinimumFileSizeKb;

type
  TFileFileSystemOptions = Set of (
      ffsoCreateFileIfNotExist,
      ffsoAutoFormatNew);
  TFileFileSystem = class(TFileSystem)
  protected
    FFileName      : String;
    FInitialSizeKb : Integer;
    FFileOptions   : TFileFileSystemOptions;

    procedure Init; override;
    procedure SetFileName(const FileName: String);
    procedure SetInitialSizeKb(const InitialSizeKb: Integer);

    procedure InitFile(var FileCreated: Boolean);
    procedure CloseFile;

  public
    property  FileName: String read FFileName write SetFileName;
    property  FileOptions: TFileFileSystemOptions read FFileOptions write FFileOptions
              default [ffsoCreateFileIfNotExist, ffsoAutoFormatNew];
    property  InitialSizeKb: Integer read FInitialSizeKb write SetInitialSizeKb
              default FS_DefaultInitialFileSizeKb;

    procedure Format(const Password: RawByteString = '';
              const Encryption: TFSEncryptionType = fseNone); override;
    procedure Open(const Password: RawByteString = ''); override;
    procedure Close; override;
  end;



{                                                                              }
{ TfsisFileSystem                                                              }
{   Published file system component.                                           }
{                                                                              }
type
  TfsifFileSystem = class(TFileFileSystem)
  published
    property  Options;
    property  PathSeparator;
    property  MaxSize;
    property  IsOpen;

    property  FileName;
    property  FileOptions;
    property  InitialSizeKb;

    property  OnWarning;
    property  OnGetPassword;
    property  OnBeforeOpen;
    property  OnOpen;
    property  OnBeforeClose;
    property  OnClose;
    property  OnGrow;
    property  OnFormatComplete;

    property  OnBeforeFileDelete;
    property  OnFileDelete;
    property  OnFileOpen;
    property  OnDirectoryMake;
    property  OnBeforeDirectoryRemove;
    property  OnDirectoryRemove;
  end;



implementation

uses
  { System }
  SysUtils,

  { Fundamentals }
  flcStrings,
  flcStringPatternMatcher,
  flcFileUtils;



{                                                                              }
{ TFileSystem                                                                  }
{                                                                              }
constructor TFileSystem.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  Init;
end;

constructor TFileSystem.CreateEx(const Stream: AStream; const StreamOwner: Boolean);
begin
  inherited Create(nil);
  Init;
  if not Assigned(Stream) then
    raise EFileSystem.Create(FSE_InvalidParamStream);
  FStream := TFileSystemReaderWriter.Create(Stream, StreamOwner);
end;

destructor TFileSystem.Destroy;
begin
  FreeAndNil(FLock);
  FreeAndNil(FStream);
  inherited Destroy;
end;

procedure TFileSystem.Init;
begin
  FLock := TCriticalSection.Create;
  FOptions := [fsoAutoGrow];
  FPathSeparator := FS_DefaultPathSeparator;
  FMaxSize := 0;
end;

procedure TFileSystem.LogWarning(const WarningCode: Integer);
begin
  if Assigned(FOnWarning) then
    FOnWarning(self, WarningCode);
end;

procedure TFileSystem.Lock;
begin
  FLock.Acquire;
end;

procedure TFileSystem.Unlock;
begin
  FLock.Release;
end;

procedure TFileSystem.SetOpen(const Open: Boolean);
begin
  if Open = FOpen then
    exit;
  if csDesigning in ComponentState then
    begin
      FOpen := Open;
      exit;
    end;
  if csLoading in ComponentState then
    begin
      FDeferredOpen := Open;
      exit;
    end;
  if Open then
    self.Open
  else
    Close;
end;

procedure TFileSystem.SetOptions(const Options: TFileSystemOptions);
begin
  Lock;
  try
    FOptions := Options;
  finally
    Unlock;
  end;
end;

procedure TFileSystem.SetPathSeparator(const PathSeparator: AnsiChar);
begin
  Lock;
  try
    FPathSeparator := PathSeparator;
  finally
    Unlock;
  end;
end;

procedure TFileSystem.Loaded;
begin
  inherited Loaded;
  if FDeferredOpen then
    Open;
end;

function TFileSystem.InitEncryption(const Password: RawByteString;
    const Encryption: TFSEncryptionType): Boolean;
begin
  Assert(Assigned(FStream));
  Result := FStream.InitEncryption(Password, Encryption);
  if not Result then
    begin
      FillChar(FPasswordHash, Sizeof(FPasswordHash), #0);
      exit;
    end;
  FPasswordHash := CalcMD5(Password);
  Result := True;
end;



{ Header block                                                                 }
procedure TFileSystem.InitHeader;
begin
  FS_InitFileSystemHeaderBlock(FHeader);
end;

procedure TFileSystem.ReadHeader;
begin
  Lock;
  try
    Assert(Assigned(FStream));
    FStream.ReadHeaderBlock(FHeader);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.WriteHeader;
begin
  Lock;
  try
    FHeader.UpdateTime := Now;
  finally
    Unlock;
  end;
  Assert(Assigned(FStream));
  FStream.WriteHeaderBlock(FHeader);
end;

function TFileSystem.GetGUID(const Idx: Integer): Word32;
begin
  CheckOpen;
  if (Idx < 0) or (Idx > 3) then
    Result := 0
  else
    Result := FHeader.GUID[Idx];
end;

function TFileSystem.GetGUIDStr: RawByteString;
begin
  CheckOpen;
  Result := FS_GUIDToStr(FHeader.GUID);
end;

function TFileSystem.GetFileSystemName: RawByteString;
begin
  CheckOpen;
  Result := FHeader.Name;
end;

procedure TFileSystem.SetFileSystemName(const Name: RawByteString);
begin
  Lock;
  try
    CheckOpen;
    FHeader.Name := CopyLeftB(Name, FS_MaxFSNameLength);
    WriteHeader;
  finally
    Unlock;
  end;
end;

function TFileSystem.GetUserData: RawByteString;
begin
  CheckOpen;
  Result := FHeader.UserData;
end;

procedure TFileSystem.SetUserData(const UserData: RawByteString);
begin
  Lock;
  try
    CheckOpen;
    FHeader.UserData := CopyLeftB(UserData, FS_MaxFSUserDataLength);
    WriteHeader;
  finally
    Unlock;
  end;
end;



{ Free blocks                                                                  }
function TFileSystem.FindFreeBlock(var Info: TFSFreeInfo): Int64;
var F : PFreeBlock;
    I : Int64;
    J : Integer;
    B : TFileSystemBlock;
begin
  Result := FS_InvalidBlock;
  Info.FreeBlock := FS_InvalidBlock;
  Info.FreeEntry := FS_InvalidFreeEntry;
  I := FHeader.FreeBlock;
  if I = FS_InvalidBlock then
    exit;
  repeat
    B := FStream.GetFreeBlock(I, F, [baReadOnlyAccess]);
    try
      J := FS_FindUsedEntryInFreeBlock(F^);
      if J >= 0 then
        begin
          Result := F^.Entries[J];
          Info.FreeBlock := I;
          Info.FreeEntry := J;
          exit;
        end;
    finally
      FStream.ReleaseFreeBlock(B, [baReadOnlyAccess], []);
    end;
    I := F^.Header.Next;
  until I = FS_InvalidBlock;
end;

function TFileSystem.ReleaseFreeBlock: Int64;
var F : TFSFreeInfo;
    B : TFileSystemBlock;
    P : PFreeBlock;
begin
  Lock;
  try
    Result := FindFreeBlock(F);
    if Result <> FS_InvalidBlock then
      begin
        Assert(F.FreeBlock <> FS_InvalidBlock, 'F.FreeBlock <> FS_InvalidBlock');
        Assert(F.FreeEntry < FS_FreeEntriesPerBlock, 'F.FreeEntry < FS_FreeEntriesPerBlock');
        B := FStream.GetFreeBlock(F.FreeBlock, P, []);
        try
          P^.Entries[F.FreeEntry] := FS_InvalidBlock;
          Dec(P^.Header.EntriesUsed);
        except
          FStream.ReleaseFreeBlock(B, [], [brDataInvalidated]);
          raise;
        end;
        FStream.ReleaseFreeBlock(B, [], [brDataChanged, brNoLazyWrite]);
      end;
  finally
    Unlock;
  end;
end;

function TFileSystem.RequireFreeBlock: Int64;
begin
  Result := ReleaseFreeBlock;
  if (Result = FS_InvalidBlock) and (fsoAutoGrow in FOptions) then
    begin
      Grow;
      Result := ReleaseFreeBlock;
    end;
  if Result = FS_InvalidBlock then
    raise EFileSystem.Create(FSE_OutOfFreeSpace);
end;

function TFileSystem.FindAvailFreeEntry(var Info: TFSFreeInfo): Boolean;
var F : Int64;
    I : Integer;
    B : TFileSystemBlock;
    P : PFreeBlock;
begin
  F := FHeader.FreeBlock;
  repeat
    if F = FS_InvalidBlock then
      begin
        Info.FreeBlock := FS_InvalidBlock;
        Info.FreeEntry := FS_InvalidFreeEntry;
        Result := False;
        exit;
      end;
    B := FStream.GetFreeBlock(F, P, [baReadOnlyAccess]);
    try
      I := FS_FindAvailEntryInFreeBlock(P^);
      if I < 0 then
        F := P^.Header.Next;
    finally
      FStream.ReleaseFreeBlock(B, [baReadOnlyAccess], []);
    end;
  until I >= 0;
  Info.FreeBlock := F;
  Info.FreeEntry := I;
  Result := True;
end;

procedure TFileSystem.AddFreeBlock(const Block: Int64);
var B : TFileSystemBlock;
    I : TFSFreeInfo;
    F : PFreeBlock;
begin
  Assert(Block <> FS_InvalidBlock, 'Block <> FS_InvalidBlock');
  Lock;
  try
    // Find available entry
    if FindAvailFreeEntry(I) then
      begin
        Assert(I.FreeBlock <> FS_InvalidBlock, 'I.FreeBlock <> FS_InvalidBlock');
        Assert(I.FreeEntry < FS_FreeEntriesPerBlock, 'I.FreeEntry < FS_FreeEntriesPerBlock');
        B := FStream.GetFreeBlock(I.FreeBlock, F, []);
        try
          Inc(F^.Header.EntriesUsed);
          F^.Entries[I.FreeEntry] := Block;
        except
          FStream.ReleaseFreeBlock(B, [], [brDataInvalidated]);
          raise;
        end;
        FStream.ReleaseFreeBlock(B, [], [brDataChanged, brNoLazyWrite]);
        exit;
      end;
    // Add free block
    B := FStream.GetBlock(Block, False, [baDontRead]);
    try
      F := PFreeBlock(B.Data);
      FS_InitFreeBlock(F^);
      F^.Header.Next := FHeader.FreeBlock;
      F^.Header.EntriesUsed := 0;
    except
      FStream.ReleaseBlock(B, [baDontRead], [brDataInvalidated]);
      raise;
    end;
    FStream.ReleaseBlock(B, [baDontRead], [brDataChanged, brNoLazyWrite]);
    FHeader.FreeBlock := Block;
    WriteHeader;
  finally
    Unlock;
  end;
end;



{ Directory blocks                                                             }
function DirectoryEntrySpecMatch(const Spec: TFSEntrySpec;
    const Entry: TDirectoryEntry): Boolean;
begin
  Result :=
     ((Spec.AttrMask = []) or (Entry.Flags * Spec.AttrMask = Spec.ReqAttr)) and
     ((Spec.Name = '')     or FS_IsDirectoryEntryName(Entry, Spec.Name)   ) and
     ((Spec.Mask = '')     or MatchFileMaskB(Spec.Mask, Entry.FileName)   );
end;

function TFileSystem.FindDirectoryEntry(const Spec: TFSEntrySpec;
    out Entry: TFSEntryInfo): Boolean;
var B : TFileSystemBlock;
    E : PDirectoryEntry;
    I : Integer;
    D : PDirectoryBlock;
    N : Int64;
    F : Word;
begin
  Entry.DirBlock := Spec.DirBlock;
  Entry.DirEntry := Spec.DirEntry;
  Result := False;
  if Spec.DirBlock = FS_InvalidBlock then
    exit;
  F := Spec.DirEntry;
  repeat
    B := FStream.GetDirectoryBlock(Entry.DirBlock, D, [baReadOnlyAccess]);
    try
      for I := F to FS_DirectoryEntriesPerBlock - 1 do
        begin
          E := @D^.Entries[I];
          if DirectoryEntrySpecMatch(Spec, E^) then
            begin
              Entry.DirEntry := I;
              Entry.Entry := E^;
              Result := True;
              exit;
            end;
        end;
      N := D^.Header.Next;
      if N <> FS_InvalidBlock then
        Entry.DirBlock := N;
    finally
      FStream.ReleaseDirectoryBlock(B, [baReadOnlyAccess], []);
    end;
    F := 0;
  until N = FS_InvalidBlock;
end;

function TFileSystem.FindUnusedDirectoryEntry(const Directory: Int64;
    out Entry: TFSEntryInfo): Boolean;
var Spec : TFSEntrySpec;
begin
  Assert(Directory <> FS_InvalidBlock, 'Directory <> FS_InvalidBlock');

  Spec.DirBlock := Directory;
  Spec.DirEntry := 0;
  Spec.Name := '';
  Spec.Mask := '';
  Spec.AttrMask := [defUsed];
  Spec.ReqAttr := [];
  Result := FindDirectoryEntry(Spec, Entry);

  Entry.Directory := Directory;
end;

function TFileSystem.FindDirectoryEntryByName(const Directory: Int64; const Name: RawByteString;
    var Entry: TFSEntryInfo): Boolean;
var Spec : TFSEntrySpec;
begin
  Assert(Directory <> FS_InvalidBlock, 'Directory <> FS_InvalidBlock');
  Spec.DirBlock := Directory;
  Spec.DirEntry := 0;
  Spec.Name := Name;
  Spec.Mask := '';
  Spec.AttrMask := [defUsed];
  Spec.ReqAttr := [defUsed];
  Result := FindDirectoryEntry(Spec, Entry);
end;

procedure TFileSystem.DeleteDirectoryEntry(const Entry: TFSEntryInfo);
var B : TFileSystemBlock;
    D : PDirectoryBlock;
begin
  Assert(Entry.DirBlock <> FS_InvalidBlock, 'Entry.DirBlock <> FS_InvalidBlock');
  Assert(Entry.DirEntry < FS_DirectoryEntriesPerBlock, 'Entry.DirEntry < FS_DirectoryEntriesPerBlock');

  B := FStream.GetDirectoryBlock(Entry.DirBlock, D, []);
  try
    FS_InitDirectoryEntry(D^.Entries[Entry.DirEntry]);
  except
    FStream.ReleaseDirectoryBlock(B, [], [brDataInvalidated]);
    raise;
  end;
  FStream.ReleaseDirectoryBlock(B, [], [brDataChanged, brNoLazyWrite]);
end;

procedure TFileSystem.RenameDirectoryEntry(const Entry: TFSEntryInfo; const Name: RawByteString);
var B : TFileSystemBlock;
    D : PDirectoryBlock;
    E : TFSEntryInfo;
begin
  Assert(Entry.DirBlock <> FS_InvalidBlock, 'Entry.DirBlock <> FS_InvalidBlock');
  Assert(Entry.DirEntry < FS_DirectoryEntriesPerBlock, 'Entry.DirEntry < FS_DirectoryEntriesPerBlock');
  FS_VerifyValidFilename (Name, FPathSeparator);
  Lock;
  try
    if FindDirectoryEntryByName(Entry.Directory, Name, E) then
      raise EFileSystem.Create(FSE_RenameFailedNameExists);
    B := FStream.GetDirectoryBlock(Entry.DirBlock, D, []);
    D^.Entries[Entry.DirEntry].FileName := Name;
    FStream.ReleaseDirectoryBlock(B, [], [brDataChanged, brNoLazyWrite]);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.AddDirectoryEntry(const Directory: Int64;
          const Entry: TDirectoryEntry; var Info: TFSEntryInfo);
var B, C : TFileSystemBlock;
    P, D : PDirectoryBlock;
    V    : Int64;
begin
  Assert(Directory <> FS_InvalidBlock, 'Directory <> FS_InvalidBlock');

  // Find unused
  if FindUnusedDirectoryEntry(Directory, Info) then
    begin
      Assert(Info.DirBlock <> FS_InvalidBlock, 'Info.DirBlock <> FS_InvalidBlock');
      Assert(Info.DirEntry < FS_DirectoryEntriesPerBlock, 'Info.DirEntry < FS_DirectoryEntriesPerBlock');
      B := FStream.GetDirectoryBlock(Info.DirBlock, D, []);
      try
        D^.Entries[Info.DirEntry] := Entry;
      except
        FStream.ReleaseDirectoryBlock(B, [], [brDataInvalidated]);
        raise;
      end;
      FStream.ReleaseDirectoryBlock(B, [], [brDataChanged, brNoLazyWrite]);
      Info.Entry := Entry;
      exit;
    end;

  // Allocate directory block
  C := FStream.GetDirectoryBlock(Info.DirBlock, P, []);
  try
    V := RequireFreeBlock;
    try
      B := FStream.GetBlock(V, False, [baDontRead]);
      try
        D := PDirectoryBlock(B.Data);
        FS_InitDirectoryBlock(D^);
        D^.Header.Next := FS_InvalidBlock;
        D^.Entries[0] := Entry;
        P^.Header.Next := V;
      except
        FStream.ReleaseDirectoryBlock(B, [baDontRead], [brDataInvalidated]);
        raise;
      end;
      FStream.ReleaseDirectoryBlock(B, [baDontRead], [brDataChanged, brNoLazyWrite]);
    except
      // Release allocated block
      try
        AddFreeBlock(V);
      except
      end;
      raise;
    end;
  except
    FStream.ReleaseDirectoryBlock(C, [], [brDataInvalidated]);
    raise;
  end;
  FStream.ReleaseDirectoryBlock(C, [], [brDataChanged, brNoLazyWrite]);
  Info.DirBlock := V;
  Info.DirEntry := 0;
  Info.Entry    := Entry;
end;

procedure TFileSystem.WriteDirectoryEntry(const Entry: TFSEntryInfo);
var B : TFileSystemBlock;
    D : PDirectoryBlock;
begin
  Assert(Entry.DirBlock <> FS_InvalidBlock, 'Entry.DirBlock <> FS_InvalidBlock');
  Assert(Entry.DirEntry < FS_DirectoryEntriesPerBlock, 'Entry.DirEntry < FS_DirectoryEntriesPerBlock');
  B := FStream.GetDirectoryBlock(Entry.DirBlock, D, []);
  try
    D^.Entries[Entry.DirEntry] := Entry.Entry;
  except
    FStream.ReleaseDirectoryBlock(B, [], [brDataInvalidated]);
    raise;
  end;
  FStream.ReleaseDirectoryBlock(B, [], [brDataChanged, brNoLazyWrite]);
end;

function TFileSystem.FindRelDirectory(const Directory: Int64; const RelPath: RawByteString): Int64;
var S, T, U : RawByteString;
    D       : Int64;
    I       : TFSEntryInfo;
    C       : TFSEntrySpec;
begin
  D := Directory;
  U := RelPath;
  C.Mask := '';
  C.AttrMask := [defUsed, defDirectory];
  C.ReqAttr := [defUsed, defDirectory];
  repeat
    if (D = FS_InvalidBlock) or (U = '') then
      begin
        Result := D;
        exit;
      end;
    PathSplitLeftElementB(U, S, T, FPathSeparator);
    if S = '' then
      raise EFileSystem.Create(FSE_InvalidParamPath);
    C.DirBlock := D;
    C.DirEntry := 0;
    C.Name := S;
    if not FindDirectoryEntry(C, I) then
      begin
        Result := FS_InvalidBlock;
        exit;
      end;
    D := I.Entry.DirBlock;
    U := T;
  until False;
end;

function TFileSystem.FindDirectory(const Directory: RawByteString): Int64;
var S : RawByteString;
begin
  Result := FHeader.RootDirBlock;
  S := Directory;
  if S = '' then
    exit;
  if S[1] = FPathSeparator then
    System.Delete(S, 1, 1);
  if S = '' then
    exit;
  Result := FindRelDirectory(Result, S);
end;

function TFileSystem.FindFile(const FileName: RawByteString; var Info: TFSEntryInfo): Boolean;
var S, T : RawByteString;
    C    : TFSEntrySpec;
begin
  Result := False;
  Info.Directory := FS_InvalidBlock;
  Info.DirBlock  := FS_InvalidDirEntry;
  DecodeFilePathB(FileName, S, T, FPathSeparator);
  if T = '' then
    exit;
  Info.Directory := FindDirectory(S);
  if Info.Directory = FS_InvalidBlock then
    exit;
  C.DirBlock := Info.Directory;
  C.DirEntry := 0;
  C.Name := T;
  C.Mask := '';
  C.AttrMask := [defUsed, defDirectory];
  C.ReqAttr := [defUsed];
  Result := FindDirectoryEntry(C, Info);
end;

function TFileSystem.FindDirectoryInfo(const Directory: RawByteString;
    var Info: TFSEntryInfo): Boolean;
var S, T, U : RawByteString;
    C       : TFSEntrySpec;
begin
  Result := False;
  Info.Directory := FS_InvalidBlock;
  Info.DirBlock  := FS_InvalidDirEntry;
  U := Directory;
  U := PathExclSuffixB(U, FPathSeparator);
  DecodeFilePathB(U, S, T, FPathSeparator);
  if T = '' then
    exit;
  Info.Directory := FindDirectory(S);
  if Info.Directory = FS_InvalidBlock then
    exit;
  C.DirBlock := Info.Directory;
  C.DirEntry := 0;
  C.Name := T;
  C.Mask := '';
  C.AttrMask := [defUsed, defDirectory];
  C.ReqAttr := [defUsed, defDirectory];
  Result := FindDirectoryEntry(C, Info);
end;



{ Allocation blocks                                                            }
procedure TFileSystem.GetAllocEntry(var Info: TFSAllocationInfo);
var A, C : PAllocationBlock;
    B, D : TFileSystemBlock;
    I    : Integer;
    P    : Int64;
begin
  Info.DataBlock := 0;
  Info.Offset := 0;
  Lock;
  try
    // Find entry in existing block
    P := FHeader.AllocBlock;
    if P <> FS_InvalidBlock then
      begin
        D := FStream.GetAllocationBlock(P, C, []);
        I := FS_FindAvailEntryInAllocationBlock(C^);
        if I >= 0 then
          begin
            try
              FS_InitAllocationEntry(C^.Entries[I]);
              Inc(C^.Header.EntriesUsed);
              C^.Entries[I].Flags := [aefUsed];
            except
              FStream.ReleaseAllocationBlock(D, [], [brDataInvalidated]);
              raise;
            end;
            FStream.ReleaseAllocationBlock(D, [], [brDataChanged, brNoLazyWrite]);
            Info.AllocBlock := P;
            Info.AllocEntry := I;
            exit;
          end;
      end
    else
      begin
        C := nil;
        D := nil;
      end;

    // Add new allocation block
    try
      Info.AllocBlock := RequireFreeBlock;
      try
        Info.AllocEntry := 0;
        B := FStream.GetBlock(Info.AllocBlock, False, []);
        try
          A := PAllocationBlock(B.Data);
          FS_InitAllocationBlock(A^);
          A^.Header.Prev := FS_InvalidBlock;
          A^.Header.Next := P;
          A^.Header.EntriesUsed := 1;
          A^.Entries[0].Flags := [aefUsed];
        except
          FStream.ReleaseBlock(B, [], [brDataInvalidated]);
          raise;
        end;
        FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
        if Assigned(C) then
          C^.Header.Prev := Info.AllocBlock;
      except
        // Release allocated block
        try
          AddFreeBlock(Info.AllocBlock);
        except
        end;
        raise;
      end;
    except
      if Assigned(D) then
        FStream.ReleaseAllocationBlock(D, [], [brDataInvalidated]);
      raise;
    end;
    if Assigned(D) then
      FStream.ReleaseAllocationBlock(D, [], []);

    // Update header
    Assert(Info.AllocBlock <> FS_InvalidBlock, 'Info.AllocBlock <> FS_InvalidBlock');
    FHeader.AllocBlock := Info.AllocBlock;
    WriteHeader;
  finally
    Unlock;
  end;
end;

procedure TFileSystem.FreeAllocEntry(const Info: TFSAllocationInfo);
var B, BP, BN : TFileSystemBlock;
    A, AP, AN : PAllocationBlock;
    I : Integer;
    F : Int64;
    E : PAllocationEntry;
begin
  Assert(Info.AllocBlock <> FS_InvalidBlock, 'Info.AllocBlock <> FS_InvalidBlock');
  Assert(Info.AllocEntry < FS_AllocationEntriesPerBlock, 'Info.AllocEntry < FS_AllocationEntriesPerBlock');
  Lock;
  try
    // Free entry
    B := FStream.GetAllocationBlock(Info.AllocBlock, A, []);
    try
      E := @A^.Entries[Info.AllocEntry];
      for I := 0 to FS_DataBlocksPerAllocationEntry - 1 do
        begin
          F := E^.DataBlocks[I];
          if F <> FS_InvalidBlock then
            begin
              E^.DataBlocks[I] := FS_InvalidBlock;
              AddFreeBlock(F);
            end;
        end;
      FS_InitAllocationEntry(E^);
      Dec(A^.Header.EntriesUsed);

      // Free block and remove from chain
      if A^.Header.EntriesUsed = 0 then
        begin
          BP := nil;
          BN := nil;
          try
            if A^.Header.Prev <> FS_InvalidBlock then
              begin
                BP := FStream.GetAllocationBlock(A^.Header.Prev, AP, []);
                AP^.Header.Next := A^.Header.Next;
              end;
            if A^.Header.Next <> FS_InvalidBlock then
              begin
                BN := FStream.GetAllocationBlock(A^.Header.Next, AN, []);
                AN^.Header.Prev := A^.Header.Prev;
              end;
          except
            if Assigned(BN) then
              FStream.ReleaseAllocationBlock(BN, [], [brDataInvalidated]);
            if Assigned(BP) then
              FStream.ReleaseAllocationBlock(BP, [], [brDataInvalidated]);
            raise;
          end;
          if Assigned(BN) then
            FStream.ReleaseAllocationBlock(BN, [], [brDataChanged, brNoLazyWrite]);
          if Assigned(BP) then
            FStream.ReleaseAllocationBlock(BP, [], [brDataChanged, brNoLazyWrite]);
          if FHeader.AllocBlock = Info.AllocBlock then
            begin
              FHeader.AllocBlock := A^.Header.Next;
              WriteHeader;
            end;
          AddFreeBlock(Info.AllocBlock);
        end;
    except
      FStream.ReleaseAllocationBlock(B, [], [brDataInvalidated]);
      raise;
    end;
    FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.ResizeAllocEntry(var Info: TFSAllocationInfo; const Size: Integer);
var OldSize, D : Integer;
    A          : PAllocationBlock;
    B          : TFileSystemBlock;
    E          : PAllocationEntry;
    I          : Int64;
begin
  Assert(Info.AllocBlock <> FS_InvalidBlock, 'Info.AllocBlock <> FS_InvalidBlock');
  Assert(Info.AllocEntry < FS_AllocationEntriesPerBlock, 'Info.AllocEntry < FS_AllocationEntriesPerBlock');
  Assert(Info.DataBlock < FS_DataBlocksPerAllocationEntry, 'Info.DataBlock < FS_DataBlocksPerAllocationEntry');
  Assert(Info.Offset <= FS_BlockSize, 'Info.Offset <= FS_BlockSize');
  Assert(Size <= FS_DataBytesPerAllocationEntry, 'Size <= FS_DataBytesPerAllocationEntry');
  Assert(Size >= 0, 'Size >= 0');
  OldSize := Info.DataBlock * FS_BlockSize + Info.Offset;
  if Size = OldSize then
    exit;
  // Quick resize on last data block
  if Size < OldSize then
    begin
      D := OldSize - Size;
      if Word32(D) <= Info.Offset then
        begin
          Dec(Info.Offset, D);
          exit;
        end;
    end
  else
    begin
      D := Size - OldSize;
      if Word32(D) <= FS_BlockSize - Info.Offset then
        begin
          Inc(Info.Offset, D);
          exit;
        end;
    end;
  // Resize DataBlocks
  Lock;
  try
    B := FStream.GetAllocationBlock(Info.AllocBlock, A, []);
    try
      E := @A^.Entries[Info.AllocEntry];
      if Size < OldSize then // shrink
        repeat
          if Word32(D) < Info.Offset then
            begin
              Dec(Info.Offset, D);
              exit;
            end;
          Dec(D, Info.Offset);
          AddFreeBlock(E^.DataBlocks[Info.DataBlock]);
          E^.DataBlocks[Info.DataBlock] := FS_InvalidBlock;
          if Info.DataBlock = 0 then
            begin
              Assert(D = 0, 'D = 0');
              Info.Offset := 0;
              exit;
            end
          else
            Dec(Info.DataBlock);
          Info.Offset := FS_BlockSize;
        until False
      else // grow
        begin
          repeat
            if Word32(D) <= FS_BlockSize - Info.Offset then
              begin
                Inc(Info.Offset, D);
                exit;
              end;
            Dec(D, FS_BlockSize - Info.Offset);
            I := RequireFreeBlock;
            Inc(Info.DataBlock);
            Assert(Info.DataBlock < FS_DataBlocksPerAllocationEntry, 'Info.DataBlock < FS_DataBlocksPerAllocationEntry');
            E^.DataBlocks[Info.DataBlock] := I;
            Info.Offset := 0;
          until False;
        end;
    finally
      FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
    end;
  finally
    Unlock;
  end;
end;



{ Allocation chains                                                            }
procedure TFileSystem.GetAllocInfo(const FileInfo: TFSEntryInfo;
    const Offset: Int64; var AllocInfo: TFSAllocationInfo);
var E : PAllocationEntry;
    N : Int64;
    L : Int64;
    A : PAllocationBlock;
    B : TFileSystemBlock;
    F : Boolean;

  procedure SetEntryOffset(const Offset: Integer);
    begin
      AllocInfo.DataBlock := Offset div FS_BlockSize;
      AllocInfo.Offset    := Offset mod FS_BlockSize;
      if F and (AllocInfo.Offset = 0) and (AllocInfo.DataBlock > 0) then
        begin
          Dec(AllocInfo.DataBlock);
          AllocInfo.Offset := FS_BlockSize;
        end;
    end;

begin
  Assert(Offset >= 0, 'Offset >= 0');
  Assert(FileInfo.Entry.Size >= 0, 'FileInfo.Entry.Size >= 0');
  Assert(Offset <= FileInfo.Entry.Size, 'Offset <= FileInfo.Entry.Size');
  AllocInfo.AllocBlock := FileInfo.Entry.AllocBlock;
  AllocInfo.AllocEntry := FileInfo.Entry.AllocEntry;
  AllocInfo.DataBlock  := FS_InvalidDataBlockEntry;
  AllocInfo.Offset     := $FFFFFFFF;
  if AllocInfo.AllocBlock = FS_InvalidBlock then
    exit;
  F := Offset >= FileInfo.Entry.Size; // EOF
  if F then
    L := FileInfo.Entry.Size else
    L := Offset;
  // Quick AllocInfo from first AllocBlock
  if Offset <= 0 then
    begin
      AllocInfo.DataBlock := 0;
      AllocInfo.Offset    := 0;
      exit;
    end else
  if Offset < FS_DataBytesPerAllocationEntry then
    begin
      SetEntryOffset(Integer(Offset));
      exit;
    end else
  if (Offset = FS_DataBytesPerAllocationEntry) and F then
    begin
      AllocInfo.DataBlock := FS_DataBlocksPerAllocationEntry - 1;
      AllocInfo.Offset    := FS_BlockSize;
      exit;
    end;
  // Walk chain
  Lock;
  try
    repeat
      Assert(L >= FS_DataBytesPerAllocationEntry, 'L >= FS_DataBytesPerAllocationEntry');
      B := FStream.GetAllocationBlock(AllocInfo.AllocBlock, A, [baReadOnlyAccess]);
      try
        repeat
          Dec(L, FS_DataBytesPerAllocationEntry);
          {$IFDEF DEBUG}
          Assert(AllocInfo.AllocEntry < FS_AllocationEntriesPerBlock, 'AllocInfo.AllocEntry < FS_AllocationEntriesPerBlock');
          {$ENDIF}
          E := @A^.Entries[AllocInfo.AllocEntry];
          N := E^.NextAllocBlock;
          if N = FS_InvalidBlock then
            begin
              if L > FS_DataBytesPerAllocationEntry then
                raise EFileSystem.Create(FSE_InvalidAllocationChain);
              if L = FS_DataBytesPerAllocationEntry then
                begin
                  AllocInfo.DataBlock := FS_DataBlocksPerAllocationEntry - 1;
                  AllocInfo.Offset    := FS_BlockSize;
                  exit;
                end;
              Assert(L < FS_DataBytesPerAllocationEntry, 'L < FS_DataBytesPerAllocationEntry');
              if L <= 0 then
                begin
                  AllocInfo.DataBlock := FS_DataBlocksPerAllocationEntry - 1;
                  AllocInfo.Offset    := FS_BlockSize;
                  exit;
                end;
              SetEntryOffset(L);
              exit;
            end
          else
            AllocInfo.AllocEntry := E^.NextAllocEntry;
        until (N <> AllocInfo.AllocBlock) or (L < FS_DataBytesPerAllocationEntry);
        AllocInfo.AllocBlock := N;
      finally
        FStream.ReleaseAllocationBlock(B, [baReadOnlyAccess], []);
      end;
    until L < FS_DataBytesPerAllocationEntry;
  finally
    Unlock;
  end;
  Assert(N <> FS_InvalidBlock, 'N <> FS_InvalidBlock');
  SetEntryOffset(L);
end;

procedure TFileSystem.GrowAlloc(var FileInfo: TFSEntryInfo;
    var AllocInfo: TFSAllocationInfo; const Size: Int64);
var D, E, F : Int64;
    J, K    : TFSAllocationInfo;
    P       : PFSAllocationInfo;
    B       : TFileSystemBlock;
    A       : PAllocationBlock;
begin
  D := Size - FileInfo.Entry.Size;
  if D <= 0 then
    exit;
  P := @AllocInfo;
  Lock;
  try
    repeat
      // Resize allocation block
      if P^.AllocBlock <> FS_InvalidBlock then
        begin
          E := P^.DataBlock * FS_BlockSize + P^.Offset;
          F := FS_DataBytesPerAllocationEntry - E;
          if F > D then
            F := D;
          ResizeAllocEntry(P^, E + F);
          Dec(D, F);
          Inc(FileInfo.Entry.Size, F);
          if D = 0 then
            begin
              Inc(E, F);
              AllocInfo.AllocBlock := P^.AllocBlock;
              AllocInfo.AllocEntry := P^.AllocEntry;
              AllocInfo.DataBlock  := E div FS_BlockSize;
              AllocInfo.Offset     := E mod FS_BlockSize;
              exit;
            end;
        end;
      // Allocate allocation block
      GetAllocEntry(J);
      // Set previous' next
      if P^.AllocBlock = FS_InvalidBlock then
        begin
          // Set first
          FileInfo.Entry.AllocBlock := J.AllocBlock;
          FileInfo.Entry.AllocEntry := J.AllocEntry;
        end
      else
        begin
          B := FStream.GetAllocationBlock(P^.AllocBlock, A, []);
          With A^.Entries[P^.AllocEntry] do
            begin
              NextAllocBlock := J.AllocBlock;
              NextAllocEntry := J.AllocEntry;
            end;
          FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
        end;
      // Update allocation entry and allocate data block
      B := FStream.GetAllocationBlock(J.AllocBlock, A, []);
      With A^.Entries[J.AllocEntry] do
        begin
          PrevAllocBlock := P^.AllocBlock;
          PrevAllocEntry := P^.AllocEntry;
          NextAllocBlock := FS_InvalidBlock;
          NextAllocEntry := FS_InvalidAllocEntry;
          DataBlocks[J.DataBlock] := RequireFreeBlock;
        end;
      FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
      K := J;
      P := @K;
      P^.DataBlock := 0;
      P^.Offset    := 0;
    until False;
  finally
    Unlock;
  end;
end;

procedure TFileSystem.ShrinkAlloc(var FileInfo: TFSEntryInfo;
    var AllocInfo: TFSAllocationInfo; const Size: Int64);
var D, E, F : Int64;
    B       : TFileSystemBlock;
    A       : PAllocationBlock;
    PB      : Int64;
    PE      : Word;
begin
  D := FileInfo.Entry.Size - Size;
  if D <= 0 then
    exit;
  Lock;
  try
    repeat
      if AllocInfo.AllocBlock = FS_InvalidBlock then
        raise EFileSystem.Create(FSE_InvalidAllocationBlock);
      E := AllocInfo.DataBlock * FS_BlockSize + AllocInfo.Offset;
      if E <= D then // Delete block
        begin
          // Find previous
          B := FStream.GetAllocationBlock(AllocInfo.AllocBlock, A, [baReadOnlyAccess]);
          With A^.Entries[AllocInfo.AllocEntry] do
            begin
              PB := PrevAllocBlock;
              PE := PrevAllocEntry;
            end;
          FStream.ReleaseAllocationBlock(B, [baReadOnlyAccess], []);
          // Update previous
          if PB = FS_InvalidBlock then // first
            begin
              FileInfo.Entry.AllocBlock := FS_InvalidBlock;
              FileInfo.Entry.AllocEntry := FS_InvalidAllocEntry;
            end
          else
            begin // previous
              B := FStream.GetAllocationBlock(PB, A, []);
              With A^.Entries[PE] do
                begin
                  NextAllocBlock := FS_InvalidBlock;
                  NextAllocEntry := FS_InvalidAllocEntry;
                end;
              FStream.ReleaseAllocationBlock(B, [], [brDataChanged, brNoLazyWrite]);
            end;
          // Free block
          FreeAllocEntry(AllocInfo);
          AllocInfo.AllocBlock := PB;
          AllocInfo.AllocEntry := PE;
          AllocInfo.DataBlock  := FS_DataBlocksPerAllocationEntry - 1;
          AllocInfo.Offset     := FS_BlockSize;
          F := E;
        end
      else
        begin
          Dec(E, D);
          ResizeAllocEntry(AllocInfo, E);
          AllocInfo.DataBlock := E div FS_BlockSize;
          AllocInfo.Offset    := E mod FS_BlockSize;
          F := D;
        end;
      Dec(D, F);
      Dec(FileInfo.Entry.Size, F);
    until D <= 0;
  finally
    Unlock;
  end;
end;

procedure TFileSystem.ResizeAlloc(var FileInfo: TFSEntryInfo; const Size: Int64);
var Info : TFSAllocationInfo;
    B    : TFileSystemBlock;
    D    : PDirectoryBlock;
begin
  Assert(Size >= 0, 'Size >= 0');
  if Size = FileInfo.Entry.Size then
    exit;
  Lock;
  try
    B := FStream.GetDirectoryBlock(FileInfo.DirBlock, D, []);
    try
      GetAllocInfo(FileInfo, FileInfo.Entry.Size, Info);
      if Size > FileInfo.Entry.Size then
        GrowAlloc (FileInfo, Info, Size)
      else
        ShrinkAlloc (FileInfo, Info, Size);
      D^.Entries[FileInfo.DirEntry] := FileInfo.Entry;
    finally
      FStream.ReleaseDirectoryBlock(B, [], [brDataChanged, brNoLazyWrite]);
    end;
  finally
    Unlock;
  end;
end;




{ Format                                                                       }
procedure TFileSystem.Format(const Password: RawByteString;
    const Encryption: TFSEncryptionType);
var Size    : Int64;
    I, J, L : Int64;
    C       : Int64;
    D       : Integer;
    U, V    : Int64;
    F       : TFreeBlock;
    R       : TDirectoryBlock;
    P       : RawByteString;
begin
  Assert(Assigned(FStream));

  Lock;
  try
    if FOpen then
      raise EFileSystem.Create(FSE_FormatFailedFileSystemOpen);

    InitHeader;

    // Initialize encryption
    FUseEncryption := InitEncryption(Password, Encryption);
    if not FUseEncryption and Assigned(FOnGetPassword) and (Encryption <> fseNone) then
      begin
        P := '';
        FOnGetPassword(self, P);
        FUseEncryption := InitEncryption(P, Encryption);
        if P <> '' then
          FillChar(Pointer(P)^, Length(P), #$FF);
      end;
    if FUseEncryption then
      begin
        Include(FHeader.Flags, fsEncrypted);
        FHeader.Encryption := Encryption;
        FHeader.PasswordHash := FPasswordHash;
        raise EFileSystem.Create(FSE_EncryptionTypeNotSupported,  'Encryption not supported');
      end
    else
      begin
        Exclude(FHeader.Flags, fsEncrypted);
        FHeader.Encryption := fseNone;
      end;
    FStream.SetEncryption(FUseEncryption);

    // Initialize TotalBlocks
    Size := FStream.Stream.Size;
    if Size < FS_MinimumSize then
      raise EFileSystem.Create(FSE_FormatFailedFileSystemTooSmall);
    if Size > FS_MaximumSize then
      Size := FS_MaximumSize;
    FHeader.TotalBlocks := Size div FS_BlockSize;

    // Initialize free blocks
    FHeader.FreeBlock := 1;
    U := 1;
    L := FHeader.TotalBlocks - 1;
    I := L;
    J := (I + FS_FreeEntriesPerBlock - 1) div FS_FreeEntriesPerBlock;
    I := I - J;
    J := (I + FS_FreeEntriesPerBlock - 1) div FS_FreeEntriesPerBlock;
    V := J + 1;
    Dec(L, J);
    I := 0;
    While I <= J - 1 do
      begin
        FS_InitFreeBlock(F);
        if L > FS_FreeEntriesPerBlock then
          begin
            C := FS_FreeEntriesPerBlock;
            F.Header.Next := I + 2;
          end
        else
          begin
            C := L;
            F.Header.Next := FS_InvalidBlock;
          end;
        F.Header.EntriesUsed := C;
        for D := 0 to C - 1 do
          F.Entries[D] := V + D;
        for D := C to FS_FreeEntriesPerBlock - 1 do
          F.Entries[D] := FS_InvalidBlock;
        Inc(V, C);
        Dec(L, C);
        FStream.StreamWriteBlock(U, F, Sizeof(TFreeBlock), False);
        Inc(U);
        Inc(I);
      end;

    // Create root directory
    I := RequireFreeBlock;
    FHeader.RootDirBlock := I;
    FS_InitDirectoryBlock(R);
    FStream.StreamWriteBlock(I, R, Sizeof(TDirectoryBlock), False);

    FHeader.AllocBlock := FS_InvalidBlock;
    FHeader.CreationTime := Now;

    WriteHeader;

    // Flush
    FStream.Flush;
  finally
    Unlock;
  end;
  if Assigned(FOnFormatComplete) then
    FOnFormatComplete(self);
end;



{ Open / Close                                                                 }
procedure TFileSystem.Open(const Password: RawByteString);
var I : Integer;
    R : Boolean;
    P : RawByteString;
begin
  Assert(Assigned(FStream));
  Lock;
  try
    if FOpen then
      exit;
    if Assigned(FOnBeforeOpen) then
      FOnBeforeOpen(self);
    // Read file system header
    ReadHeader;
    if fsOpenSession in FHeader.Flags then
      LogWarning(FSW_PreviousSessionOpen);
    if FHeader.MajorVersion > FileSystemMajorVersion then
      LogWarning(FSW_UnsupportedFileSystemVersion) else
      if (FHeader.HeaderSize > Sizeof(TFileSystemHeaderBlock)) or
         (FHeader.CreationTime = 0.0) then
        LogWarning(FSW_NonStandardFileSystemHeader);
    if FHeader.RootDirBlock = FS_InvalidBlock then
      raise EFileSystem.Create(FSE_NoRootDirectory);
    // Initialize encryption
    if (fsEncrypted in FHeader.Flags) and (FHeader.Encryption <> fseNone) then
      begin
        R := InitEncryption(Password, FHeader.Encryption);
        if not R and Assigned(FOnGetPassword) then
          begin
            P := '';
            FOnGetPassword(self, P);
            R := InitEncryption(P, FHeader.Encryption);
            if Length(P) > 0 then
              FillChar(Pointer(P)^, Length(P), #0);
          end;
        if not R then
          raise EFileSystem.Create(FSE_EncryptionPasswordRequired);
        for I := 0 to Sizeof(FPasswordHash) - 1 do
          if FPasswordHash.Bytes[I] <> FHeader.PasswordHash.Bytes[I] then
            raise EFileSystem.Create(FSE_EncryptionInvalidPassword);
        FUseEncryption := True;
      end
    else
      FUseEncryption := False;
    FStream.SetEncryption(FUseEncryption);
    // Write file system header
    Include(FHeader.Flags, fsOpenSession);
    WriteHeader;
    // Set opened
    FOpen := True;
    if Assigned(FOnOpen) then
      FOnOpen(self);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.Close;
begin
  Lock;
  try
    if not FOpen then
      exit;
    if Assigned(FOnBeforeClose) then
      FOnBeforeClose(self);
    // Update files
    if Assigned(FStream) then
      begin
        FStream.Flush;
        Exclude(FHeader.Flags, fsOpenSession);
        WriteHeader;
      end;
    // Set closed
    FOpen := False;
    if Assigned(FOnClose) then
      FOnClose(self);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.CheckOpen;
begin
  if not FOpen then
    raise EFileSystem.Create(FSE_InvalidStateFileSystemNotOpen);
end;

procedure TFileSystem.CheckNotOpen;
begin
  if FOpen then
    raise EFileSystem.Create(FSE_InvalidStateFileSystemOpen);
end;



{ Stream size                                                                  }
function TFileSystem.GetSize: Int64;
begin
  CheckOpen;

  Assert(Assigned(FStream));
  Result := FStream.StreamSize;
end;

procedure TFileSystem.SetSize(const Size: Int64);
var OldSize   : Int64;
    Blocks, I : Int64;
begin
  if Size < 0 then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  if (FMaxSize > 0) and (Size > FMaxSize) then
    raise EFileSystem.Create(FSE_InvalidParamSize);

  Lock;
  try
    CheckOpen;

    Assert(Assigned(FStream));
    OldSize := FStream.StreamSize;

    if Size = OldSize then
      exit;

    if Size > OldSize then // grow
      begin
        Blocks := Size div FS_BlockSize;
        if Blocks < FHeader.TotalBlocks then
          raise EFileSystem.Create(FSE_InvalidFileSystemSize);
        FStream.StreamSize := Size;
        if Blocks = FHeader.TotalBlocks then
          exit;
        try
          I := FHeader.TotalBlocks;
          While I <= Blocks - 1 do
            begin
              AddFreeBlock(I);
              FHeader.TotalBlocks := I + 1;
              Inc(I);
            end;
        finally
          WriteHeader;
        end;
        exit;
      end;

    // shrinking not supported
    raise EFileSystem.Create(FSE_InvalidParamSize);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.Grow;
const MaximumGrowSize   = 256 * 1024; // 256 Kb
      MaximumGrowBlocks = MaximumGrowSize div FS_BlockSize; // 128
var Blocks              : Int64;
    OldSize, NewSize, N : Int64;
begin
  Lock;
  try
    CheckOpen;
    Blocks := FHeader.TotalBlocks div 10; // Default grow 10%
    if Blocks < FS_MinimumBlocks then
      Blocks := FS_MinimumBlocks else // Grow at least 4 blocks (8 Kb)
    if Blocks > MaximumGrowBlocks then
      Blocks := MaximumGrowBlocks; // Grow at most 256 Kb
    NewSize := (FHeader.TotalBlocks + Blocks) * FS_BlockSize;
    if Assigned(FOnGrow) then
      begin
        OldSize := FHeader.TotalBlocks * FS_BlockSize;
        N := NewSize;
        FOnGrow(self, OldSize, N);
        if N <= OldSize then
          exit;
        NewSize := N;
      end;
    SetSize(NewSize);
  finally
    Unlock;
  end;
end;



{ File information                                                             }
function TFileSystem.FileExists(const FileName: RawByteString): Boolean;
var E : TFSEntryInfo;
begin
  CheckOpen;
  Result := FindFile(FileName, E);
end;

function TFileSystem.GetFileFlags(const FileName: RawByteString): TDirectoryEntryFlags;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := []
  else
    Result := E.Entry.Flags;
end;

procedure TFileSystem.SetFileFlags(const FileName: RawByteString; const Flags: TDirectoryEntryFlags);
var E : TFSEntryInfo;
    F : TDirectoryEntryFlags;
begin
  CheckOpen;
  Lock;
  try
    if not FindFile(FileName, E) then
      raise EFileSystem.Create(FSE_FileNotFound);
    F := Flags + [defUsed];
    if not (defDirectory in E.Entry.Flags) then
      Exclude (F, defDirectory);
    if F = Flags then
      exit;
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    E.Entry.Flags := F;
    WriteDirectoryEntry(E);
  finally
    Unlock;
  end;
end;

function TFileSystem.GetFileSize(const FileName: RawByteString): Int64;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := -1
  else
    Result := E.Entry.Size;
end;

function TFileSystem.GetFileCreateTime(const FileName: RawByteString): TDateTime;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := 0.0
  else
    Result := E.Entry.CreateTime;
end;

function TFileSystem.GetFileAccessTime(const FileName: RawByteString): TDateTime;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := 0.0
  else
    Result := E.Entry.AccessTime;
end;

function TFileSystem.GetFileModifyTime(const FileName: RawByteString): TDateTime;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := 0.0
  else
    Result := E.Entry.ModifyTime;
end;

function TFileSystem.GetFileGUID(const FileName: RawByteString): FS_GUID;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    FillChar(Result, Sizeof(FS_GUID), #0)
  else
    Result := E.Entry.GUID;
end;

function TFileSystem.GetFileGUIDStr(const FileName: RawByteString): RawByteString;
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    Result := ''
  else
    Result := FS_GUIDToStr(E.Entry.GUID);
end;

procedure TFileSystem.GetFileContentType(const FileName: RawByteString; var ContentType: TFSContentType);
var E : TFSEntryInfo;
begin
  CheckOpen;
  if not FindFile(FileName, E) then
    raise EFileSystem.Create(FSE_FileNotFound);
  ContentType := E.Entry.ContentType;
end;

procedure TFileSystem.SetFileContentType(const FileName: RawByteString; const ContentType: TFSContentType);
var E : TFSEntryInfo;
begin
  CheckOpen;
  Lock;
  try
    if not FindFile(FileName, E) then
      raise EFileSystem.Create(FSE_FileNotFound);
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    E.Entry.ContentType := ContentType;
    WriteDirectoryEntry(E);
  finally
    Unlock;
  end;
end;



{ TFSIterator                                                                  }
constructor TFSIterator.Create(const FileSystem: TFileSystem; const Spec: TFSEntrySpec);
begin
  inherited Create;
  Assert(Assigned(FileSystem), 'Assigned(FileSystem)');
  FFileSystem := FileSystem;
  FSpec := Spec;
  FEntry.DirBlock := FS_InvalidBlock;
  FindFirst;
end;

procedure TFSIterator.FindFirst;
begin
  if not FFileSystem.FindDirectoryEntry(FSpec, FEntry) then
    FEntry.DirBlock := FS_InvalidBlock;
end;

function TFSIterator.EOF: Boolean;
begin
  Result := FEntry.DirBlock = FS_InvalidBlock;
end;

procedure TFSIterator.CheckValidEntry;
begin
  if EOF then
    raise EFileSystem.Create(FSE_IteratorNoEntry);
end;

procedure TFSIterator.FindNext;
begin
  CheckValidEntry;
  FSpec.DirBlock := FEntry.DirBlock;
  FSpec.DirEntry := FEntry.DirEntry + 1;
  if not FFileSystem.FindDirectoryEntry(FSpec, FEntry) then
    FEntry.DirBlock := FS_InvalidBlock;
end;

function TFSIterator.GetName: RawByteString;
begin
  CheckValidEntry;
  Result := FEntry.Entry.FileName;
end;

function TFSIterator.GetSize: Int64;
begin
  CheckValidEntry;
  Result := FEntry.Entry.Size;
end;

function TFSIterator.GetFlags: TDirectoryEntryFlags;
begin
  CheckValidEntry;
  Result := FEntry.Entry.Flags;
end;

function TFSIterator.GetCreateTime: TDateTime;
begin
  CheckValidEntry;
  Result := FEntry.Entry.CreateTime;
end;

function TFSIterator.GetAccessTime: TDateTime;
begin
  CheckValidEntry;
  Result := FEntry.Entry.AccessTime;
end;

function TFSIterator.GetModifyTime: TDateTime;
begin
  CheckValidEntry;
  Result := FEntry.Entry.ModifyTime;
end;

procedure TFSIterator.GetContentType(var ContentType: TFSContentType);
begin
  CheckValidEntry;
  ContentType := FEntry.Entry.ContentType;
end;



{ FindFirst                                                                    }
function TFileSystem.FindFirst(const Path, Mask: RawByteString; const AttrMask,
    ReqAttr: TDirectoryEntryFlags): TFSIterator;
var S : TFSEntrySpec;
begin
  CheckOpen;
  S.DirBlock := FindDirectory(Path);
  if S.DirBlock = FS_InvalidBlock then
    begin
      Result := nil; // Directory not found
      exit;
    end;
  S.DirEntry := 0;
  S.Name := '';
  S.Mask := Mask;
  S.AttrMask := AttrMask + [defUsed];
  S.ReqAttr := ReqAttr + [defUsed];
  Result := TFSIterator.Create(self, S);
end;



{ TFSOpenFile                                                                  }
constructor TFSOpenFile.Create(const FileSystem: TFileSystem; const Info: TFSEntryInfo);
begin
  inherited Create;
  Assert(Assigned(FileSystem), 'Assigned(FileSystem)');
  Assert(FInfo.DirBlock <> FS_InvalidBlock, 'FInfo.DirBlock <> FS_InvalidBlock');
  Assert(FInfo.DirEntry < FS_DirectoryEntriesPerBlock, 'FInfo.DirEntry < FS_DirectoryEntriesPerBlock');
  FFileSystem := FileSystem;
  FInfo := Info;
  FGUID := Info.Entry.GUID;
end;

destructor TFSOpenFile.Destroy;
begin
  FreeAndNil(FReader);
  FreeAndNil(FWriter);
  inherited Destroy;
end;

procedure TFSOpenFile.LockFileSystem;
begin
  Assert(Assigned(FFileSystem), 'Assigned(FFileSystem)');
  FFileSystem.Lock;
end;

procedure TFSOpenFile.UnlockFileSystem;
begin
  Assert(Assigned(FFileSystem), 'Assigned(FFileSystem)');
  FFileSystem.Unlock;
end;

procedure TFSOpenFile.ReadDirectoryEntry;
var B : TFileSystemBlock;
    D : PDirectoryBlock;
begin
  B := FFileSystem.FStream.GetDirectoryBlock(FInfo.DirBlock, D, [baReadOnlyAccess]);
  try
    FInfo.Entry := D^.Entries[FInfo.DirEntry];
    if not FS_SameGUID(FInfo.Entry.GUID, FGUID) then
      raise EFileSystem.Create(FSE_HandleInvalidated);
  finally
    FFileSystem.FStream.ReleaseDirectoryBlock(B, [baReadOnlyAccess], []);
  end;
end;

procedure TFSOpenFile.WriteDirectoryEntry(const MayCache: Boolean);
var B : TFileSystemBlock;
    D : PDirectoryBlock;
    O : TBlockReleaseOptions;
begin
  B := FFileSystem.FStream.GetDirectoryBlock(FInfo.DirBlock, D, []);
  try
    D^.Entries[FInfo.DirEntry] := FInfo.Entry;
  finally
    if MayCache then
      O := [brDataChanged] else
      O := [brDataChanged, brNoLazyWrite];
    FFileSystem.FStream.ReleaseDirectoryBlock(B, [], O);
  end;
end;

function TFSOpenFile.GetReader: AReaderEx;
begin
  if not Assigned(FReader) then
    FReader := TStreamReaderProxy.Create(self);
  Result := FReader;
end;

function TFSOpenFile.GetWriter: AWriterEx;
begin
  if not Assigned(FWriter) then
    FWriter := TStreamWriterProxy.Create(self);
  Result := FWriter;
end;

function TFSOpenFile.GetSize: Int64;
begin
  ReadDirectoryEntry;
  Result := FInfo.Entry.Size;
end;

procedure TFSOpenFile.SetSize(const Size: Int64);
begin
  if Size < 0 then
    raise EFileSystem.Create(FSE_InvalidParamSize);
  LockFileSystem;
  try
    ReadDirectoryEntry;
    FS_VerifyDirectoryEntryNotReadOnly(FInfo.Entry);
    FFileSystem.ResizeAlloc(FInfo, Size);
  finally
    UnlockFileSystem;
  end;
end;

function TFSOpenFile.GetPosition: Int64;
begin
  Result := FPosition;
end;

procedure TFSOpenFile.SetPosition(const Position: Int64);
begin
  if Position < 0 then
    raise EFileSystem.Create(FSE_InvalidParamPosition);
  if FPosition = Position then
    exit;
  FPosition := Position;
end;

function TFSOpenFile.GetGUIDStr: RawByteString;
begin
  Result := FS_GUIDToStr(FGUID);
end;

function TFSOpenFile.GetGUID(const Idx: Integer): Word32;
begin
  if (Idx < 0) or (Idx > 3) then
    Result := 0
  else
    Result := FGUID[Idx];
end;

procedure TFSOpenFile.GetContentType(var ContentType: TFSContentType);
begin
  ContentType := FInfo.Entry.ContentType;
end;

procedure TFSOpenFile.SetContentType(const ContentType: TFSContentType);
begin
  LockFileSystem;
  try
    ReadDirectoryEntry;
    FS_VerifyDirectoryEntryNotReadOnly(FInfo.Entry);
    FInfo.Entry.ContentType := ContentType;
    WriteDirectoryEntry(True);
  finally
    UnlockFileSystem;
  end;
end;

function TFSOpenFile.Read(var Buffer; const Size: Integer): Integer;
var I    : TFSAllocationInfo;
    Q    : PByte;
    L, S : Integer;
    A    : PAllocationBlock;
    B    : TFileSystemBlock;
    D    : Int64;
    E    : PAllocationEntry;
begin
  Result := 0;
  L := Size;
  if L <= 0 then
    exit;
  Q := @Buffer;
  if not Assigned(Q) then
    raise EFileSystem.Create(FSE_InvalidParamBuffer);
  LockFileSystem;
  try
    ReadDirectoryEntry;
    // Update directory entry for read access
    if not (fsoDontUpdateAccessTime in FFileSystem.FOptions) then
      begin
        FInfo.Entry.AccessTime := Now;
        Inc(FInfo.Entry.AccessCount);
        WriteDirectoryEntry(True);
      end;
    // Check size
    if L > FInfo.Entry.Size - FPosition then
      begin
        L := Integer(FInfo.Entry.Size - FPosition);
        if L <= 0 then
          exit;
      end;
    // Read chain
    FFileSystem.GetAllocInfo(FInfo, FPosition, I);
    repeat
      if I.AllocBlock = FS_InvalidBlock then
        raise EFileSystem.Create(FSE_InvalidAllocationChain);
      B := FFileSystem.FStream.GetAllocationBlock(I.AllocBlock, A, [baReadOnlyAccess]);
      try
        repeat
          E := @A^.Entries[I.AllocEntry];
          repeat
            D := E^.DataBlocks[I.DataBlock];
            if D = FS_InvalidBlock then
              exit;
            S := FS_BlockSize - I.Offset;
            if S > L then
              S := L;
            FFileSystem.FStream.ReadBlock(D, Q^, I.Offset, S);
            Inc(Q, S);
            Dec(L, S);
            Inc(FPosition, S);
            Inc(Result, S);
            if L = 0 then
              exit;
            Inc(I.DataBlock);
            I.Offset := 0;
          until I.DataBlock >= FS_DataBlocksPerAllocationEntry;
          I.DataBlock := 0;
          I.AllocEntry := E^.NextAllocEntry;
        until I.AllocBlock <> E^.NextAllocBlock;
        I.AllocBlock := E^.NextAllocBlock;
      finally
        FFileSystem.FStream.ReleaseAllocationBlock(B, [baReadOnlyAccess], []);
      end;
    until False;
  finally
    UnlockFileSystem;
  end;
end;

function TFSOpenFile.Write(const Buffer; const Size: Integer): Integer;
var I    : TFSAllocationInfo;
    Q    : PByte;
    L, S : Integer;
    A    : PAllocationBlock;
    B    : TFileSystemBlock;
    D    : Int64;
    E    : PAllocationEntry;
begin
  Result := 0;
  L := Size;
  if L <= 0 then
    exit;
  Q := @Buffer;
  if not Assigned(Q) then
    raise EFileSystem.Create(FSE_InvalidParamBuffer);
  LockFileSystem;
  try
    ReadDirectoryEntry;
    FS_VerifyDirectoryEntryNotReadOnly(FInfo.Entry);
    if FPosition + L >= FInfo.Entry.Size then
      SetSize(FPosition + L);
    // Update directory entry for write access
    FInfo.Entry.ModifyTime := Now;
    Inc(FInfo.Entry.ModifyCount);
    Include(FInfo.Entry.Flags, defModified);
    Exclude(FInfo.Entry.Flags, defHashUpdated);
    WriteDirectoryEntry(True);
    // Write chain
    FFileSystem.GetAllocInfo(FInfo, FPosition, I);
    repeat
      if I.AllocBlock = FS_InvalidBlock then
        raise EFileSystem.Create(FSE_InvalidAllocationChain);
      B := FFileSystem.FStream.GetAllocationBlock(I.AllocBlock, A, [baReadOnlyAccess]);
      try
        repeat
          E := @A^.Entries[I.AllocEntry];
          repeat
            D := E^.DataBlocks[I.DataBlock];
            if D = FS_InvalidBlock then
              exit;
            S := FS_BlockSize - I.Offset;
            if S > L then
              S := L;
            FFileSystem.FStream.WriteBlock(D, Q^, I.Offset, S);
            Inc(Q, S);
            Dec(L, S);
            Inc(FPosition, S);
            Inc(Result, S);
            if L = 0 then
              exit;
            Inc(I.DataBlock);
            I.Offset := 0;
          until I.DataBlock >= FS_DataBlocksPerAllocationEntry;
          I.DataBlock := 0;
          I.AllocEntry := E^.NextAllocEntry;
        until I.AllocBlock <> E^.NextAllocBlock;
        I.AllocBlock := E^.NextAllocBlock;
      finally
        FFileSystem.FStream.ReleaseAllocationBlock(B, [baReadOnlyAccess], []);
      end;
    until False;
  finally
    UnlockFileSystem;
  end;
end;



{ TFSFileStream                                                                }
constructor TFSFileStream.Create(const OpenFile: TFSOpenFile);
begin
  Assert(Assigned(OpenFile), 'Assigned(OpenFile)');
  inherited Create;
  FOpenFile := OpenFile;
end;

procedure TFSFileStream.SetSize(NewSize: Longint);
begin
  FOpenFile.Size := NewSize;
end;

{$IFNDEF DELPHI5_DOWN}
procedure TFSFileStream.SetSize(const NewSize: Int64);
begin
  FOpenFile.Size := NewSize;
end;
{$ENDIF}

function TFSFileStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := FOpenFile.Read(Buffer, Count);
end;

function TFSFileStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := FOpenFile.Write(Buffer, Count);
end;

function TFSFileStream.Seek(Offset: Longint; Origin: Word): Longint;
begin
  Case Origin of
    soFromBeginning : FOpenFile.Position := Offset;
    soFromCurrent   : FOpenFile.Position := FOpenFile.Position + Offset;
    soFromEnd       : FOpenFile.Position := FOpenFile.Size - Offset;
  end;
  Result := FOpenFile.Position;
end;

{$IFNDEF DELPHI5_DOWN}
function TFSFileStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  Case Origin of
    soBeginning : FOpenFile.Position := Offset;
    soCurrent   : FOpenFile.Position := FOpenFile.Position + Offset;
    soEnd       : FOpenFile.Position := FOpenFile.Size - Offset;
  end;
  Result := FOpenFile.Position;
end;
{$ENDIF}



{ OpenFile                                                                     }
function TFileSystem.OpenFile(const FileName: RawByteString;
    const OpenMode: TFSFileOpenMode): TFSOpenFile;
var E    : TFSEntryInfo;
    N    : TDirectoryEntry;
    R    : Boolean;
    S, T : RawByteString;
begin
  CheckOpen;
  Lock;
  try
    R := FindFile(FileName, E);
    Case OpenMode of
      fomOpenReadWrite,
      fomOpenReadOnly :
        begin
          if not R then
            raise EFileSystem.Create(FSE_OpenFailedFileNotFound);
          if OpenMode = fomOpenReadWrite then
            FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
        end;
      fomCreateIfNotExist,
      fomCreate :
        if R then
          begin
            FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
            if OpenMode = fomCreate then
              ResizeAlloc(E, 0);
          end
        else
          begin
            DecodeFilePathB(FileName, S, T, FPathSeparator);
            E.Directory := FindDirectory(S);
            if E.Directory = FS_InvalidBlock then
              raise EFileSystem.Create(FSE_OpenFailedDirectoryNotFound);
            FS_InitNewDirectoryEntry(N);
            N.FileName := T;
            AddDirectoryEntry(E.Directory, N, E);
          end;
    else
      raise EFileSystem.Create(FSE_OpenFailedInvalidMode);
    end;
  finally
    Unlock;
  end;
  Result := TFSOpenFile.Create(self, E);
  if Assigned(FOnFileOpen) then
    FOnFileOpen(self, FileName);
end;

function TFileSystem.OpenFileStream(const FileName: RawByteString;
    const OpenMode: TFSFileOpenMode): TFSFileStream;
begin
  Result := TFSFileStream.Create(OpenFile(FileName, OpenMode));
end;



{ DeleteFile                                                                   }
procedure TFileSystem.DeleteFile(const FileName: RawByteString);
var E : TFSEntryInfo;
    R : Boolean;
begin
  CheckOpen;
  Lock;
  try
    if not FindFile(FileName, E) then
      exit;
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    if Assigned(FOnBeforeFileDelete) then
      begin
        R := True;
        FOnBeforeFileDelete(self, FileName, R);
        if not R then
          raise EFileSystem.Create(FSE_OperationCancelled);
      end;
    ResizeAlloc(E, 0);
    DeleteDirectoryEntry(E);
    if Assigned(FOnFileDelete) then
      FOnFileDelete(self, FileName);
  finally
    Unlock;
  end;
end;



{ RenameFile                                                                   }
procedure TFileSystem.RenameFile(const FileName, Name: RawByteString);
var E : TFSEntryInfo;
begin
  CheckOpen;
  Lock;
  try
    if not FindFile(FileName, E) then
      raise EFileSystem.Create(FSE_InvalidParamFilename);
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    RenameDirectoryEntry(E, Name);
  finally
    Unlock;
  end;
end;



{ Directories                                                                  }
procedure TFileSystem.MakeDirectory(const Directory: RawByteString);
var S, P, T : RawByteString;
    N       : TDirectoryEntry;
    I       : TFSEntryInfo;
    B       : TDirectoryBlock;
begin
  CheckOpen;
  S := Directory;
  S := PathExclSuffixB(S, FPathSeparator);
  if S = '' then
    raise EFileSystem.Create(FSE_InvalidParamPath);
  DecodeFilePathB(S, P, T, FPathSeparator);
  if FindDirectoryInfo(Directory, I) then
    raise EFileSystem.Create(FSE_MakeDirFailedNameExists);
  if I.Directory = FS_InvalidBlock then
    raise EFileSystem.Create(FSE_MakeDirFailedDirectoryNotExist);
  if T = '' then
    raise EFileSystem.Create(FSE_InvalidParamPath);
  FS_InitNewDirectoryEntry(N);
  Include (N.Flags, defDirectory);
  N.FileName := T;
  N.DirBlock := RequireFreeBlock;
  FS_InitDirectoryBlock(B);
  Lock;
  try
    FStream.RemoveCached(N.DirBlock);
    FStream.StreamWriteBlock(N.DirBlock, B, Sizeof(TDirectoryBlock), False);
    AddDirectoryEntry(I.Directory, N, I);
  finally
    Unlock;
  end;
  if Assigned(FOnDirectoryMake) then
    FOnDirectoryMake(self, Directory);
end;

procedure TFileSystem.RemoveDirectory(const Directory: RawByteString);
var D, N : Int64;
    E    : TFSEntryInfo;
    B    : TFileSystemBlock;
    P    : PDirectoryBlock;
    R    : Boolean;
begin
  CheckOpen;
  Lock;
  try
    D := FindDirectory(Directory);
    if D = FS_InvalidBlock then
      raise EFileSystem.Create(FSE_InvalidParamPath);
    if FindDirectoryEntryByName(D, '', E) then
      raise EFileSystem.Create(FSE_DirectoryNotEmpty);
    if not FindDirectoryInfo(Directory, E) then
      raise EFileSystem.Create(FSE_InvalidParamPath);
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    if Assigned(FOnBeforeDirectoryRemove) then
      begin
        R := True;
        FOnBeforeDirectoryRemove(self, Directory, R);
        if not R then
          raise EFileSystem.Create(FSE_OperationCancelled);
      end;
    // Free directory blocks
    repeat
      B := FStream.GetDirectoryBlock(D, P, [baReadOnlyAccess]);
      N := P^.Header.Next;
      FStream.ReleaseDirectoryBlock(B, [baReadOnlyAccess], []);
      AddFreeBlock(D);
      D := N;
    until N = FS_InvalidBlock;

    DeleteDirectoryEntry(E);
  finally
    Unlock;
  end;
  if Assigned(FOnDirectoryRemove) then
    FOnDirectoryRemove(self, Directory);
end;

function TFileSystem.DirectoryExists(const Directory: RawByteString): Boolean;
begin
  CheckOpen;
  Result := FindDirectory(Directory) <> FS_InvalidBlock;
end;

procedure TFileSystem.RenameDirectory(const Directory, Name: RawByteString);
var E : TFSEntryInfo;
begin
  CheckOpen;
  Lock;
  try
    if not FindDirectoryInfo(Directory, E) then
      raise EFileSystem.Create(FSE_InvalidParamPath);
    FS_VerifyDirectoryEntryNotReadOnly(E.Entry);
    RenameDirectoryEntry(E, Name);
  finally
    Unlock;
  end;
end;

procedure TFileSystem.EnsurePathExists(const Directory: RawByteString);
var S    : RawByteString;
    I, L : Integer;
begin
  if DirectoryExists(Directory) then
    exit;
  I := 0;
  L := Length(Directory);
  repeat
    I := PosCharB(FPathSeparator, Directory, I + 1);
    if (I = 0) or (I = L) then
      begin
        MakeDirectory(Directory);
        exit;
      end else
    if I > 1 then
      begin
        S := CopyLeftB(Directory, I - 1);
        if not DirectoryExists(S) then
          MakeDirectory(S);
      end;
  until False;
end;



{                                                                              }
{ TFileFileSystem                                                              }
{                                                                              }
procedure TFileFileSystem.Init;
begin
  inherited Init;
  FInitialSizeKb := FS_DefaultInitialFileSizeKb;
  FFileOptions := [ffsoCreateFileIfNotExist, ffsoAutoFormatNew];
end;

procedure TFileFileSystem.SetFileName(const FileName: String);
begin
  Lock;
  try
    if FileName = FFileName then
      exit;
    if [csDesigning, csLoading] * ComponentState = [] then
      CheckNotOpen;
    FFileName := FileName;
  finally
    Unlock;
  end;
end;

procedure TFileFileSystem.SetInitialSizeKb(const InitialSizeKb: Integer);
var I : Integer;
begin
  Lock;
  try
    if InitialSizeKb < FS_MinimumFileSizeKb then
      I := FS_MinimumFileSizeKb
    else
      I := InitialSizeKb;
    if I = FInitialSizeKb then
      exit;
    if [csDesigning, csLoading] * ComponentState = [] then
      CheckNotOpen;
    FInitialSizeKb := I;
  finally
    Unlock;
  end;
end;

procedure TFileFileSystem.InitFile(var FileCreated: Boolean);
var F : TFileStream;
    M : TFileStreamOpenMode;
begin
  FileCreated := False;
  if FFileName = '' then
    raise EFileSystem.Create(FSE_InvalidParamFilename);
  if ffsoCreateFileIfNotExist in FFileOptions then
    M := fsomCreateIfNotExist else
    M := fsomReadWrite;
  F := TFileStream.Create(FFileName, M, [fsoWriteThrough], fsahNone);
  try
    FileCreated := F.FileCreated;
    if FileCreated and (FInitialSizeKb > 0) then
      F.Size := FInitialSizeKb * 1024;
  except
    F.Free;
    raise;
  end;
  FStream := TFileSystemReaderWriter.Create(F, True);
end;

procedure TFileFileSystem.CloseFile;
begin
  FreeAndNil(FStream);
end;

procedure TFileFileSystem.Format(const Password: RawByteString;
    const Encryption: TFSEncryptionType);
var R : Boolean;
begin
  Lock;
  try
    if FOpen then
      raise EFileSystem.Create(FSE_FormatFailedFileSystemOpen);
    InitFile(R);
    try
      inherited Format(Password, Encryption);
    finally
      CloseFile;
    end;
  finally
    Unlock;
  end;
end;

procedure TFileFileSystem.Open(const Password: RawByteString);
var R : Boolean;
begin
  Lock;
  try
    if FOpen then
      exit;
    InitFile(R);
    try
      if R and (ffsoAutoFormatNew in FFileOptions) then
        inherited Format(Password);
      inherited Open(Password);
    except
      FOpen := False;
      CloseFile;
      raise;
    end;
  finally
    Unlock;
  end;
end;

procedure TFileFileSystem.Close;
begin
  Lock;
  try
    if not FOpen then
      exit;
    CloseFile;
    inherited Close;
  finally
    Unlock;
  end;
end;



end.


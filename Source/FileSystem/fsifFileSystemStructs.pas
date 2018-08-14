(******************************************************************************)
(*                                                                            *)
(*    Description:   File System Structures                                   *)
(*    Version:       1.06                                                     *)
(*    Copyright:     Copyright (c) 2002-2018, David J Butler                  *)
(*                   All rights reserved.                                     *)
(*    License:       MIT License                                              *)
(*    Github:        https://github.com/fsif                                  *)
(*                                                                            *)
(*  Revision history:                                                         *)
(*    2002/08/09  1.00  Created cFileSystemStructs from cFileSystem.          *)
(*    2002/08/11  1.01  Added GUIDs.                                          *)
(*    2002/08/14  1.02  Added ContentType to TDirectoryEntry.                 *)
(*    2002/01/19  1.03  Added file system encryption.                         *)
(*    2003/06/23  1.04  Revision.                                             *)
(*    2003/08/26  1.05  Increased maximum file system size.                   *)
(*    2016/04/13  1.06  String changes.                                       *)
(*                                                                            *)
(******************************************************************************)

{$INCLUDE fsifFileSystem.inc}

unit fsifFileSystemStructs;

interface

uses
  { System }
  SysUtils,

  { Fundamentals }
  flcStdTypes,
  flcHash;



{                                                                              }
{ File system constants                                                        }
{                                                                              }
const
  FS_CopyrightStr              = 'Copyright (c) 2002-2016 Fundamentals Library';
  FS_VersionStr                = '1.00';
  FS_ProductStr                = 'Fundamentals File System';
  FS_ProductVersionStr         = FS_ProductStr + ' ' + FS_VersionStr;
  FS_ProductShortStr           = 'FLFS';
  FS_MajorVersion              = 1;
  FS_MinorVersion              = 0;
  FS_BlockSize                 = 2048;
  FS_MaxFileNameLength         = 255;
  FS_MaxFSNameLength           = 64;
  FS_MaxFSUserDataLength       = 255;
  FS_MaxMinorContentTypeLength = 16;
  FS_MaxContentEncodingLength  = 16;
  FS_InvalidBlock              = Int64(-1);
  FS_InvalidAllocEntry         = Word($FFFF);
  FS_InvalidFreeEntry          = Word($FFFF);
  FS_InvalidDirEntry           = Word($FFFF);
  FS_InvalidDataBlockEntry     = Word($FFFF);
  FS_MinimumBlocks             = 4;
  FS_MinimumSize               = Int64(FS_MinimumBlocks) * FS_BlockSize; // 8 KB
  FS_MaximumBlocks             = Int64($000000FFFFFFFFFF); // 40 bit index ~ 1E12 blocks
  FS_MaximumSize               = Int64(FS_MaximumBlocks) * FS_BlockSize; // 2048 TB
  FS_DefaultPathSeparator      = '/';



{                                                                              }
{ Errors                                                                       }
{                                                                              }

{ Error codes                                                                  }
const
  // Special codes
  FSE_NoError                        = $0000;

  // Warning codes
  FSW_FileSystemWarning              = $0100;
  FSW_PreviousSessionOpen            = $0101;
  FSW_UnsupportedFileSystemVersion   = $0102;
  FSW_NonStandardFileSystemHeader    = $0103;

  // Error codes
  FSE_InvalidParam                   = $1100;
  FSE_InvalidParamBlockSize          = $1101;
  FSE_InvalidParamSize               = $1102;
  FSE_InvalidParamPath               = $1103;
  FSE_InvalidParamStream             = $1104;
  FSE_InvalidParamCacheSize          = $1105;
  FSE_InvalidParamBlockIndex         = $1106;
  FSE_InvalidParamBuffer             = $1107;
  FSE_InvalidParamPosition           = $1108;
  FSE_InvalidParamFilename           = $1109;
  FSE_InvalidParamIndex              = $110A;

  FSE_InvalidFileSystemStruct        = $1200;
  FSE_InvalidFileSystemHeader        = $1210;
  FSE_InvalidFileSystemHeaderSize    = $1211;
  FSE_InvalidFileSystemSize          = $1212;
  FSE_NoRootDirectory                = $1213;
  FSE_InvalidDirectoryBlock          = $1220;
  FSE_InvalidFreeBlock               = $1230;
  FSE_InvalidAllocationBlock         = $1240;
  FSE_InvalidAllocationChain         = $1241;
  FSE_EncryptionPasswordRequired     = $1250;
  FSE_EncryptionInvalidPassword      = $1251;
  FSE_EncryptionTypeNotSupported     = $1252;

  FSE_OutOfResources                 = $1300;
  FSE_OutOfFreeSpace                 = $1301;
  FSE_OutOfBufferSpace               = $1302;

  FSE_FormatFailed                   = $1400;
  FSE_FormatFailedFileSystemOpen     = $1401;
  FSE_FormatFailedFileSystemTooSmall = $1402;

  FSE_OpenFailed                     = $1500;
  FSE_OpenFailedFileNotFound         = $1501;
  FSE_OpenFailedDirectoryNotFound    = $1502;
  FSE_OpenFailedInvalidMode          = $1503;

  FSE_BlockAccessError               = $1600;
  FSE_BlockLocked                    = $1601;

  FSE_UnsupportedFileSystemFeature   = $1700;
  FSE_UnsupportedBlockSize           = $1701;

  FSE_IteratorError                  = $1800;
  FSE_IteratorNoEntry                = $1801;
  FSE_IteratorNextPastEnd            = $1802;

  FSE_InvalidState                   = $1900;
  FSE_InvalidStateFileSystemNotOpen  = $1901;
  FSE_InvalidStateFileSystemOpen     = $1902;

  FSE_OperationFailed                = $1A00;
  FSE_OperationCancelled             = $1A10;
  FSE_DirectoryNotEmpty              = $1A20;
  FSE_RenameFailedNameExists         = $1A30;
  FSE_FileIsReadOnly                 = $1A40;
  FSE_FileNotFound                   = $1A50;
  FSE_MakeDirFailedNameExists        = $1A60;
  FSE_MakeDirFailedDirectoryNotExist = $1A61;
  FSE_HandleInvalidated              = $1A70;



{ Error messages                                                               }
function FileSystemErrorMessage(const ErrorCode: Integer): String;



{ Exception                                                                    }
type
  EFileSystem = class(Exception)
  protected
    FErrorCode : Integer;

  public
    constructor Create(const ErrorCode: Integer; const Msg: String = '');
    property ErrorCode: Integer read FErrorCode;
  end;



{                                                                              }
{ Structures                                                                   }
{                                                                              }
type
  FS_GUID = array[0..3] of Word32; // 128-bit globally unique ID

procedure FS_InitGUID(var GUID: FS_GUID);
function  FS_GUIDToStr(const GUID: FS_GUID): RawByteString;
function  FS_SameGUID(const A, B: FS_GUID): Boolean;



{                                                                              }
{ File system structure                                                        }
{                                                                              }
{   +================+                                                         }
{   | Header Block   |                                                         }
{   +----------------+                                                         }
{   | Block          |                                                         }
{   +----------------+                                                         }
{   | Block          |                                                         }
{   |   ...          |                                                         }
{   +================+                                                         }
{                                                                              }

{                                                                              }
{ Header block                                                                 }
{                                                                              }
const
  FileSystemMagic        = $001A5346;
  FileSystemMajorVersion = FS_MajorVersion;
  FileSystemMinorVersion = FS_MinorVersion;

type
  TFileSystemHeaderFlags = set of (
      fsOpenSession,
      fsEncrypted);

  TFSEncryptionType = (
      fseNone,
      fsePasswordOnly,
      fseCast256);

  TFileSystemHeaderBlock = packed record // 512 bytes
    Magic           : Word32;                          // FileSystemMagic
    MajorVersion    : Byte;                            // FS_MajorVersion (1)
    MinorVersion    : Byte;                            // FS_MinorVersion (0)
    Flags           : TFileSystemHeaderFlags;          // 1 byte
    ReservedFlags   : array[1..3] of Byte;             // Default 0
    HeaderSize      : Word32;                          // 512
    BlockSize       : Word32;                          // FS_BlockSize (2048)
    TotalBlocks     : Int64;                           // Total blocks (incl fs header)
    RootDirBlock    : Int64;                           // Root directory block
    FreeBlock       : Int64;                           // First free block
    AllocBlock      : Int64;                           // First allocation block
    GUID            : FS_GUID;                         // 128 bit globally unique id
    Name            : String[FS_MaxFSNameLength];      // File system identifier
    CreationTime    : TDateTime;                       // File system creation time
    UpdateTime      : TDateTime;                       // Time of last file system update
    Encryption      : TFSEncryptionType;               // Type of encryption
    PasswordHash    : T128BitDigest;                   // MD5 hash of password (if encrypted)
    Reserved        : array[0..91] of Byte;            // Default 0
    UserData        : String[FS_MaxFSUserDataLength];  // User defined data (256 bytes)
  end;

procedure FS_InitFileSystemHeaderBlock(var Block: TFileSystemHeaderBlock);
procedure FS_VerifyValidFileSystemHeaderBlock(const Block: TFileSystemHeaderBlock);



{                                                                              }
{ Directory block                                                              }
{                                                                              }

{ Header                                                                       }
const
  DirectoryBlockMagic = $55AA4253;

type
  TDirectoryBlockHeader = packed record // 32 bytes
    Magic    : Word32;                // DirectoryBlockMagic
    Flags    : Word32;                // Default 0
    Next     : Int64;                 // Directory block
    Reserved : array[0..15] of Byte;  // Default 0
  end;

{ Entry                                                                        }
type
  TDirectoryEntryFlags = set of (
      defUsed,
      defDirectory,
      defReadOnly,
      defModified,
      defHashUpdated,
      defCompressed,
      defEncrypted);

  TDirectoryEntryType = (
      detFile);

  TFSFileNameEncoding = (
      fneASCII,
      fneUTF8);

  TFSMajorContentType = (
      fsctUndefined,
      fsctApplication,
      fsctText,
      fsctImage,
      fsctAudio,
      fsctVideo,
      fsctExecutable,
      fsctScript);

  TFSContentType = packed record // 35 bytes
    Major    : TFSMajorContentType;                   // 1 byte major content type
    Minor    : String[FS_MaxMinorContentTypeLength];  // minor content type (ASCII string)
    Encoding : String[FS_MaxContentEncodingLength];   // content-type specific encoding (ASCII string)
  end;

  TFSHashType = (
      fshtNoHash,
      fshtMD5,
      fshtSHA1);

  TFSCompressionType = (
      fctNone,
      fctZLIB);

  TDirectoryEntry = packed record // 504 bytes
    EntryType        : TDirectoryEntryType;          // Currently only detFile (0)
    Flags            : TDirectoryEntryFlags;         // 1 byte
    ReservedFlags    : array[1..3] of Byte;          // Default 0
    FileNameEncoding : TFSFileNameEncoding;          // Default fneASCII (0)
    FileName         : String[FS_MaxFileNameLength]; // 256 bytes
    Size             : Int64;
    AllocBlock       : Int64;                        // Data allocation block
    AllocEntry       : Word;                         // Data allocation entry
    DirBlock         : Int64;                        // First directory block if entry is a directory
    CreateTime       : TDateTime;
    ModifyTime       : TDateTime;
    ModifyCount      : Word32;
    AccessTime       : TDateTime;
    AccessCount      : Word32;
    GUID             : FS_GUID;                      // 128 bit globally unique id
    ContentType      : TFSContentType;
    HashType         : TFSHashType;                  // 1 byte
    HashValue        : T160BitDigest;                // 20 bytes
    Compression      : TFSCompressionType;           // Type of file compression
    UncompressedSize : Int64;                        // Size of file when uncompressed, -1 if not compressed
    CompressParam    : Int64;                        // Default 0
    Encryption       : TFSEncryptionType;            // Type of file encryption
    PasswordHash     : T128BitDigest;                // MD5 hash of password
    Reserved         : array[0..77] of Byte;         // Default 0
  end;
  PDirectoryEntry = ^TDirectoryEntry;

const
  FS_DirectoryBlockDataSize   = FS_BlockSize - Sizeof(TDirectoryBlockHeader); // 2016 bytes
  FS_DirectoryEntriesPerBlock = FS_DirectoryBlockDataSize div Sizeof(TDirectoryEntry); // 4 entries

procedure FS_InitDirectoryEntry(var Entry: TDirectoryEntry);
procedure FS_InitNewDirectoryEntry(var Entry: TDirectoryEntry);
function  FS_IsDirectoryEntryName(const Entry: TDirectoryEntry; const Name: RawByteString): Boolean;
procedure FS_VerifyDirectoryEntryNotReadOnly(const Entry: TDirectoryEntry);

{ Block                                                                        }
type
  TDirectoryBlock = packed record
    Header  : TDirectoryBlockHeader;
    Entries : array[0..FS_DirectoryEntriesPerBlock - 1] of TDirectoryEntry;
  end;
  PDirectoryBlock = ^TDirectoryBlock;

procedure FS_InitDirectoryBlock(var Block: TDirectoryBlock);
procedure FS_VerifyValidDirectoryBlock(const Block: TDirectoryBlock);



{                                                                              }
{ Free block                                                                   }
{                                                                              }
const
  FreeBlockMagic = $53F05F42;

type
  TFreeBlockHeader = packed record // 26 bytes
    Magic       : Word32;               // FreeBlockMagic
    Flags       : Word32;               // Default 0
    Next        : Int64;                // Free block
    EntriesUsed : Word;
    Reserved    : array[0..7] of Byte;  // Default 0
  end;

const
  FS_FreeBlockDataSize   = FS_BlockSize - Sizeof(TFreeBlockHeader); // 2022 bytes
  FS_FreeEntriesPerBlock = FS_FreeBlockDataSize div Sizeof(Int64); // 252 entries

type
  TFreeBlock = packed record
    Header  : TFreeBlockHeader;
    Entries : array[0..FS_FreeEntriesPerBlock - 1] of Int64;
  end;
  PFreeBlock = ^TFreeBlock;

procedure FS_InitFreeBlock(var Block: TFreeBlock);
procedure FS_VerifyValidFreeblock(const Block: TFreeBlock);
function  FS_FindAvailEntryInFreeBlock(const Block: TFreeBlock): Integer;
function  FS_FindUsedEntryInFreeBlock(const Block: TFreeBlock): Integer;



{                                                                              }
{ Allocation block                                                             }
{                                                                              }

{ Entry                                                                        }
const
  FS_DataBlocksPerAllocationEntry = 8;
  FS_DataBytesPerAllocationEntry  = FS_DataBlocksPerAllocationEntry * FS_BlockSize; // 16 KB

type
  TAllocationEntryFlags = set of (aefUsed);

  TAllocationEntry = packed record // 85 bytes
    Flags          : TAllocationEntryFlags;
    DataBlocks     : array[0..FS_DataBlocksPerAllocationEntry - 1] of Int64;
    PrevAllocBlock : Int64;
    PrevAllocEntry : Word;
    NextAllocBlock : Int64;
    NextAllocEntry : Word;
  end;
  PAllocationEntry = ^TAllocationEntry;

procedure FS_InitAllocationEntry(var Entry: TAllocationEntry);

{ Block                                                                        }
const
  AllocationBlockMagic = $0F573422;

type
  TAllocationBlockHeader = packed record // 34 bytes
    Magic       : Word32;               // AllocationBlockMagic
    Flags       : Word32;               // Default 0
    Prev        : Int64;                // Allocation block
    Next        : Int64;                // Allocation block
    EntriesUsed : Word;
    Reserved    : array[0..7] of Byte;  // Default 0
  end;

const
  FS_AllocationBlockDataSize      = FS_BlockSize - Sizeof(TAllocationBlockHeader); // 2022 bytes
  FS_AllocationEntriesPerBlock    = FS_AllocationBlockDataSize div Sizeof(TAllocationEntry); // 23 entries
  FS_DataBlocksPerAllocationBlock = FS_DataBlocksPerAllocationEntry * FS_AllocationEntriesPerBlock; // 184 entries

type
  TAllocationBlock = packed record
    Header  : TAllocationBlockHeader;
    Entries : array[0..FS_AllocationEntriesPerBlock - 1] of TAllocationEntry;
  end;
  PAllocationBlock = ^TAllocationBlock;

procedure FS_InitAllocationBlock(var Block: TAllocationBlock);
procedure FS_VerifyValidAllocationBlock(const Block: TAllocationBlock);
function  FS_FindAvailEntryInAllocationBlock(const Block: TAllocationBlock): Integer;



{                                                                              }
{ Filename and path                                                            }
{                                                                              }
function  FS_IsValidFilename(const Filename: RawByteString;
          const PathSeparator: AnsiChar = FS_DefaultPathSeparator): Boolean;
procedure FS_VerifyValidFilename(const Filename: RawByteString;
          const PathSeparator: AnsiChar = FS_DefaultPathSeparator);



implementation

uses
  { Fundamentals }
  flcUtils,
  flcRandom;



{                                                                              }
{ Errors                                                                       }
{                                                                              }
function FileSystemErrorMessage(const ErrorCode: Integer): String;
var ErrorClass : Integer;
begin
  if ErrorCode < 0 then
    Result := 'Invalid error code' else
  if ErrorCode = FSE_NoError then
    Result := '' else
    case ErrorCode of
      FSE_InvalidParamBlockSize          : Result := 'Invalid block size';
      FSE_InvalidParamSize               : Result := 'Invalid size';
      FSE_InvalidParamPath               : Result := 'Invalid path';
      FSE_InvalidParamStream             : Result := 'Invalid stream';
      FSE_InvalidParamCacheSize          : Result := 'Invalid cache size';
      FSE_InvalidParamBlockIndex         : Result := 'Invalid block index';
      FSE_InvalidParamBuffer             : Result := 'Invalid buffer';
      FSE_InvalidParamPosition           : Result := 'Invalid position';
      FSE_InvalidParamFilename           : Result := 'Invalid filename';
      FSE_InvalidParamIndex              : Result := 'Invalid index';
      FSE_InvalidFileSystemHeader        : Result := 'Invalid file system header';
      FSE_InvalidFileSystemHeaderSize    : Result := 'Invalid file system header size';
      FSE_InvalidFileSystemSize          : Result := 'Invalid file system size';
      FSE_NoRootDirectory                : Result := 'No root directory';
      FSE_InvalidDirectoryBlock          : Result := 'Invalid directory block';
      FSE_InvalidFreeBlock               : Result := 'Invalid free block';
      FSE_InvalidAllocationBlock         : Result := 'Invalid allocation block';
      FSE_InvalidAllocationChain         : Result := 'Invalid allocation chain';
      FSE_EncryptionPasswordRequired     : Result := 'Encrypted file system: Password required';
      FSE_EncryptionInvalidPassword      : Result := 'Invalid password';
      FSE_EncryptionTypeNotSupported     : Result := 'Encryption type not supported';
      FSE_OutOfFreeSpace                 : Result := 'No free space';
      FSE_OutOfBufferSpace               : Result := 'Out of buffer space';
      FSE_FormatFailedFileSystemOpen     : Result := 'File system open';
      FSE_FormatFailedFileSystemTooSmall : Result := 'File system too small';
      FSE_OpenFailedFileNotFound         : Result := 'File not found';
      FSE_OpenFailedDirectoryNotFound    : Result := 'Directory not found';
      FSE_OpenFailedInvalidMode          : Result := 'Invalid file mode';
      FSE_BlockLocked                    : Result := 'File system block locked';
      FSE_UnsupportedBlockSize           : Result := 'Block size not supported';
      FSE_IteratorNoEntry                : Result := 'No directory entry';
      FSE_IteratorNextPastEnd            : Result := 'Next past end';
      FSE_InvalidStateFileSystemNotOpen  : Result := 'File system not open';
      FSE_InvalidStateFileSystemOpen     : Result := 'File system open';
      FSE_DirectoryNotEmpty              : Result := 'Directory not empty';
      FSE_RenameFailedNameExists         : Result := 'Name exists';
      FSE_FileIsReadOnly                 : Result := 'File is read-only';
      FSE_FileNotFound                   : Result := 'File not found';
      FSE_MakeDirFailedNameExists        : Result := 'Directory exists';
      FSE_MakeDirFailedDirectoryNotExist : Result := 'Directory not found';
      FSE_HandleInvalidated              : Result := 'Handle invalidated';
      FSE_OperationCancelled             : Result := 'Cancelled';
      FSW_PreviousSessionOpen            : Result := 'Previous session open';
      FSW_UnsupportedFileSystemVersion   : Result := 'Unsupported file system version';
      FSW_NonStandardFileSystemHeader    : Result := 'Non-standard file system header';
    else
      begin
        ErrorClass := (ErrorCode div $100) * $100;
        case ErrorClass of
          FSE_InvalidParam                 : Result := 'Invalid parameter';
          FSE_InvalidFileSystemStruct      : Result := 'Invalid file system structure';
          FSE_OutOfResources               : Result := 'Out of resources';
          FSE_FormatFailed                 : Result := 'Format failed';
          FSE_OpenFailed                   : Result := 'Open failed';
          FSE_BlockAccessError             : Result := 'File system block access error';
          FSE_UnsupportedFileSystemFeature : Result := 'File system feature not supported';
          FSE_IteratorError                : Result := 'Iterator error';
          FSE_InvalidState                 : Result := 'Invalid state';
          FSE_OperationFailed              : Result := 'Operation failed';
          FSW_FileSystemWarning            : Result := 'File system warning';
        else
          Result := 'File system error ' + IntToStr(ErrorCode);
        end;
      end;
    end;
end;

constructor EFileSystem.Create(const ErrorCode: Integer; const Msg: String);
begin
  if Msg <> '' then
    inherited Create(Msg)
  else
    inherited Create(FileSystemErrorMessage(ErrorCode));
  FErrorCode := ErrorCode;
end;



{                                                                              }
{ Structures                                                                   }
{                                                                              }
procedure FS_InitGUID(var GUID: FS_GUID);
var I : Integer;
begin
  for I := Low(FS_GUID) to High(FS_GUID) do
    GUID[I] := RandomUniform32;
end;

function FS_GUIDToStr(const GUID: FS_GUID): RawByteString;
var I : Integer;
begin
  Result := '';
  for I := Low(FS_GUID) to High(FS_GUID) do
    Result := Result + Word32ToHexB(GUID[I], 8);
end;

function FS_SameGUID(const A, B: FS_GUID): Boolean;
var I : Integer;
begin
  for I := Low(FS_GUID) to High(FS_GUID) do
    if A[I] <> B[I] then
      begin
        Result := False;
        exit;
      end;
  Result := True;
end;



{                                                                              }
{ Header block                                                                 }
{                                                                              }
procedure FS_InitFileSystemHeaderBlock(var Block: TFileSystemHeaderBlock);
begin
  FillChar(Block, Sizeof(TFileSystemHeaderBlock), 0);
  Block.Magic        := FileSystemMagic;
  Block.MajorVersion := FileSystemMajorVersion;
  Block.MinorVersion := FileSystemMinorVersion;
  Block.Flags        := [];
  Block.HeaderSize   := Sizeof(TFileSystemHeaderBlock);
  Block.BlockSize    := FS_BlockSize;
  Block.RootDirBlock := FS_InvalidBlock;
  Block.FreeBlock    := FS_InvalidBlock;
  Block.AllocBlock   := FS_InvalidBlock;
  Block.Encryption   := fseNone;
  FS_InitGUID(Block.GUID);
end;

procedure FS_VerifyValidFileSystemHeaderBlock(const Block: TFileSystemHeaderBlock);
begin
  if Block.Magic <> FileSystemMagic then
    raise EFileSystem.Create(FSE_InvalidFileSystemHeader);
  if Block.HeaderSize < Sizeof(TFileSystemHeaderBlock) then
    raise EFileSystem.Create(FSE_InvalidFileSystemHeaderSize);
  if Block.TotalBlocks = 0 then
    raise EFileSystem.Create(FSE_InvalidFileSystemSize);
  if Block.MajorVersion = 0 then
    raise EFileSystem.Create(FSE_InvalidFileSystemHeader);
  if Block.BlockSize <> FS_BlockSize then
    raise EFileSystem.Create(FSE_UnsupportedBlockSize);
end;



{                                                                              }
{ Directory blocks                                                             }
{                                                                              }
procedure FS_InitDirectoryEntry(var Entry: TDirectoryEntry);
begin
  FillChar(Entry, Sizeof(TDirectoryEntry), 0);
  with Entry do
    begin
      Flags             := [];
      EntryType         := detFile;
      FileNameEncoding  := fneUTF8;
      Size              := -1;
      AllocBlock        := FS_InvalidBlock;
      AllocEntry        := FS_InvalidAllocEntry;
      DirBlock          := FS_InvalidBlock;
      ContentType.Major := fsctUndefined;
      HashType          := fshtNoHash;
      Compression       := fctNone;
      UncompressedSize  := -1;
      Encryption        := fseNone;
    end;
end;

procedure FS_InitNewDirectoryEntry(var Entry: TDirectoryEntry);
begin
  FS_InitDirectoryEntry(Entry);
  with Entry do
    begin
      Flags      := [defUsed];
      CreateTime := Now;
      Size       := 0;
      FS_InitGUID(GUID);
    end;
end;

procedure FS_InitDirectoryBlock(var Block: TDirectoryBlock);
var I : Integer;
    P : PDirectoryEntry;
begin
  FillChar(Block, Sizeof(TDirectoryBlock), 0);
  Block.Header.Magic := DirectoryBlockMagic;
  Block.Header.Next  := FS_InvalidBlock;
  P := @Block.Entries;
  for I := 0 to FS_DirectoryEntriesPerBlock - 1 do
    begin
      FS_InitDirectoryEntry(P^);
      Inc(P);
    end;
end;

procedure FS_VerifyValidDirectoryBlock(const Block: TDirectoryBlock);
begin
  if Block.Header.Magic <> DirectoryBlockMagic then
    raise EFileSystem.Create(FSE_InvalidDirectoryBlock);
end;

function FS_IsDirectoryEntryName(const Entry: TDirectoryEntry; const Name: RawByteString): Boolean;
var L1, L2 : Integer;
    P1, P2 : Pointer;
begin
  L1 := Length(Entry.FileName);
  L2 := Length(Name);
  Result := L1 = L2;
  if not Result or (L1 = 0) then
    exit;
  P1 := @Entry.FileName[1];
  P2 := Pointer(Name);
  Result := CompareMemNoAsciiCase(P1^, P2^, L1) = 0;
end;

procedure FS_VerifyDirectoryEntryNotReadOnly(const Entry: TDirectoryEntry);
begin
  if defReadOnly in Entry.Flags then
    raise EFileSystem.Create(FSE_FileIsReadOnly);
end;



{                                                                              }
{ Free blocks                                                                  }
{                                                                              }
procedure FS_InitFreeBlock(var Block: TFreeBlock);
var I : Integer;
    P : PInt64;
begin
  FillChar(Block, Sizeof(TFreeBlock), 0);
  Block.Header.Magic := FreeBlockMagic;
  Block.Header.Next  := FS_InvalidBlock;
  P := @Block.Entries;
  for I := 0 to FS_FreeEntriesPerBlock - 1 do
    begin
      P^ := FS_InvalidBlock;
      Inc(P);
    end;
end;

procedure FS_VerifyValidFreeblock(const Block: TFreeBlock);
begin
  if Block.Header.Magic <> FreeBlockMagic then
    raise EFileSystem.Create(FSE_InvalidFreeBlock);
end;

function FS_FindAvailEntryInFreeBlock(const Block: TFreeBlock): Integer;
var I : Word;
    P : PInt64;
begin
  if Block.Header.EntriesUsed >= FS_FreeEntriesPerBlock then
    begin
      Result := -1;
      exit;
    end;
  P := @Block.Entries;
  for I := 0 to FS_FreeEntriesPerBlock - 1 do
    if P^ = FS_InvalidBlock then
      begin
        Result := I;
        exit;
      end else
      Inc(P);
  Result := -1;
end;

function FS_FindUsedEntryInFreeBlock(const Block: TFreeBlock): Integer;
var I : Word;
    P : PInt64;
begin
  if Block.Header.EntriesUsed = 0 then
    begin
      Result := -1;
      exit;
    end;
  P := @Block.Entries;
  for I := 0 to FS_FreeEntriesPerBlock - 1 do
    if P^ <> FS_InvalidBlock then
      begin
        Result := I;
        exit;
      end else
      Inc(P);
  Result := -1;
end;



{                                                                              }
{ Allocation blocks                                                            }
{                                                                              }
procedure FS_InitAllocationEntry(var Entry: TAllocationEntry);
var P : PInt64;
    I : Integer;
begin
  FillChar(Entry, Sizeof(TAllocationEntry), 0);
  Entry.PrevAllocBlock := FS_InvalidBlock;
  Entry.PrevAllocEntry := FS_InvalidAllocEntry;
  Entry.NextAllocBlock := FS_InvalidBlock;
  Entry.NextAllocEntry := FS_InvalidAllocEntry;
  P := @Entry.DataBlocks;
  for I := 0 to FS_DataBlocksPerAllocationEntry - 1 do
    begin
      P^ := FS_InvalidBlock;
      Inc(P);
    end;
end;

procedure FS_InitAllocationBlock(var Block: TAllocationBlock);
var I : Integer;
    Q : PAllocationEntry;
begin
  FillChar(Block, Sizeof(TAllocationBlock), 0);
  Block.Header.Magic := AllocationBlockMagic;
  Block.Header.Prev  := FS_InvalidBlock;
  Block.Header.Next  := FS_InvalidBlock;
  Q := @Block.Entries;
  for I := 0 to FS_AllocationEntriesPerBlock - 1 do
    begin
      FS_InitAllocationEntry(Q^);
      Inc(Q);
    end;
end;

procedure FS_VerifyValidAllocationBlock(const Block: TAllocationBlock);
begin
  if Block.Header.Magic <> AllocationBlockMagic then
    raise EFileSystem.Create(FSE_InvalidAllocationBlock);
end;

function FS_FindAvailEntryInAllocationBlock(const Block: TAllocationBlock): Integer;
var I : Integer;
    P : PAllocationEntry;
begin
  if Block.Header.EntriesUsed >= FS_AllocationEntriesPerBlock then
    begin
      Result := -1;
      exit;
    end;
  P := @Block.Entries;
  for I := 0 to FS_AllocationEntriesPerBlock - 1 do
    if not (aefUsed in P^.Flags) then
      begin
        Result := I;
        exit;
      end else
      Inc(P);
  Result := -1;
end;



{                                                                              }
{ Filename and path                                                            }
{                                                                              }
function FS_IsValidFilename(const Filename: RawByteString; const PathSeparator: AnsiChar): Boolean;
var P : PByte;
    L : Integer;
    C : ByteCharSet;
begin
  Result := False;
  L := Length(Filename);
  if L = 0 then
    exit;
  P := Pointer(Filename);
  C := [PathSeparator, #0];
  repeat
    if AnsiChar(P^) in C then
      exit;
    Inc(P);
    Dec(L);
  until L = 0;
  Result := True;
end;

procedure FS_VerifyValidFilename(const Filename: RawByteString; const PathSeparator: AnsiChar);
begin
  if not FS_IsValidFilename(Filename, PathSeparator) then
    raise EFileSystem.Create(FSE_InvalidParamFilename);
end;



end.


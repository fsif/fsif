{$INCLUDE fsifFileSystem.inc}

unit fsifFileSystemTests;

interface



{                                                                              }
{ Tests                                                                        }
{                                                                              }
procedure Test;
procedure Profile;



implementation

uses
  SysUtils,
  { Fundamentals }
  flcStdTypes,
  flcUtils,
  flcStrings,
  flcTimers,
  flcStreams,
  flcFileUtils,
  { FileSystem }
  fsifFileSystemStructs,
  fsifFileSystem;



{                                                                              }
{ Self-testing code                                                            }
{                                                                              }
{$ASSERTIONS ON}
procedure TestStructs;
var G : FS_GUID;
begin
  Assert(FSE_NoError = 0, 'FSE_NoError = 0');
  Assert(FileSystemErrorMessage(FSE_NoError) = '', 'FileSystemErrorMessage(FSE_NoError) = ''''');
  Assert(FileSystemErrorMessage(1) <> '', 'FileSystemErrorMessage(1) <> ''''');

  G[0] := $01234567;
  G[1] := $89ABCDEF;
  G[2] := $A55A0FF0;
  G[3] := $000000FF;
  Assert(FS_GUIDToStr(G) = '0123456789ABCDEFA55A0FF0000000FF', 'FS_GUIDToStr');

  Assert(Sizeof(TFileSystemHeaderBlock) = 512, 'Sizeof(TFileSystemHeaderBlock) = 512');
  Assert(FS_DirectoryEntriesPerBlock = 4, 'FS_DirectoryEntriesPerBlock = 4');
  Assert(Sizeof(TDirectoryEntry) = 504, 'Sizeof(TDirectoryEntry) = 504');
  Assert(Sizeof(TDirectoryBlock) = FS_BlockSize, 'Sizeof(TDirectoryBlock) = FS_BlockSize');
  Assert(Sizeof(TAllocationBlock) <= FS_BlockSize, 'Sizeof(TAllocationBlock) <= FS_BlockSize');
  Assert(Sizeof(TFreeBlock) <= FS_BlockSize, 'Sizeof(TFreeBlock) <= FS_BlockSize');

  Assert(FS_IsValidFilename('test.file'), 'FS_IsValidFilename');
  Assert(FS_IsValidFilename('test'), 'FS_IsValidFilename');
  Assert(not FS_IsValidFilename(''), 'FS_IsValidFilename');
  Assert(not FS_IsValidFilename('/'), 'FS_IsValidFilename');
  Assert(not FS_IsValidFilename('/test.file'), 'FS_IsValidFilename');
  Assert(not FS_IsValidFilename('test/file'), 'FS_IsValidFilename');
  Assert(not FS_IsValidFilename(#0), 'FS_IsValidFilename');
end;

procedure TestDirOps(const FS : TFileSystem; const Dir : RawByteString);
var I : TFSIterator;
begin
  // MakeDirectory, DirectoryExists
  Assert(FS.DirectoryExists(Dir), 'DirectoryExists');
  Assert(not FS.DirectoryExists(Dir + 'test'), 'DirectoryExists');
  Assert(not FS.DirectoryExists(Dir + 'test/'), 'DirectoryExists');
  Assert(not FS.DirectoryExists(Dir + 'test.file'), 'DirectoryExists');
  FS.MakeDirectory(Dir + 'test');
  Assert(FS.DirectoryExists(Dir + 'test'), 'DirectoryExists');
  Assert(FS.DirectoryExists(Dir + 'test/'), 'DirectoryExists');

  // FindFirst
  I := FS.FindFirst(Dir, 't?st', [defDirectory], [defDirectory]);
  Assert(Assigned(I), 'FindFirst');
  Assert(not I.EOF, 'Iterator.EOF');
  Assert(I.Name = 'test', 'Iterator.FileName');
  Assert(defDirectory in I.Flags, 'Iterator.Flags');
  I.FindNext;
  Assert(I.EOF, 'Iterator.EOF');
  I.Free;

  // RenameDirectory
  FS.RenameDirectory(Dir + 'test', 'abc');
  Assert(FS.DirectoryExists(Dir + 'abc'), 'DirectoryExists');
  Assert(not FS.DirectoryExists(Dir + 'test'), 'DirectoryExists');
  FS.RenameDirectory(Dir + 'abc', 'test');
  Assert(not FS.DirectoryExists(Dir + 'abc'), 'DirectoryExists');
  Assert(FS.DirectoryExists(Dir + 'test'), 'DirectoryExists');

  // RemoveDirectory
  FS.RemoveDirectory(Dir + 'test');
  Assert(not FS.DirectoryExists(Dir + 'test'), 'DirectoryExists');
  Assert(not FS.DirectoryExists(Dir + 'abc'), 'DirectoryExists');
end;

procedure TestFileOps(const FS : TFileSystem; const Dir, FileName : RawByteString);
var FH  : TFSOpenFile;
    Buf : RawByteString;
    I   : TFSIterator;
begin
  // File Create, File Write
  Assert(not FS.FileExists(Dir + FileName), 'FileExists');
  FH := FS.OpenFile(Dir + FileName, fomCreate);
  Assert(FH.Position = 0, 'File.Position');
  Assert(FH.Size = 0, 'File.Size');
  FH.Size := 10;
  Assert(FH.Position = 0, 'File.Position');
  Assert(FH.Size = 10, 'File.Size');
  Buf := '0123456789';
  Assert(FH.Write(Pointer(Buf)^, 10) = 10, 'File.Write');
  Assert(FH.Position = 10, 'File.Position');
  FH.Free;

  // FileExists
  Assert(FS.FileExists(Dir + FileName), 'FileExists');
  Assert(FS.GetFileSize(Dir + FileName) = 10, 'FileSize');

  // File Open, File Read
  FH := FS.OpenFile(Dir + FileName, fomOpenReadWrite);
  Assert(FH.Position = 0, 'File.Position');
  Assert(FH.Size = 10, 'File.Size');
  SetLength(Buf, 10);
  FillChar(Pointer(Buf)^, 10, 'X');
  Assert(FH.Read(Pointer(Buf)^, 3) = 3, 'File.Read');
  Assert(Buf = '012XXXXXXX', 'File.Read');
  Assert(FH.Position = 3, 'File.Position');
  Assert(FH.Read(Pointer(Buf)^, 1) = 1, 'File.Read');
  Assert(Buf = '312XXXXXXX', 'File.Read');
  FH.Position := 0;
  Assert(FH.Position = 0, 'File.Position');
  Assert(FH.Read(Pointer(Buf)^, 11) = 10, 'File.Read');
  Assert(FH.Position = 10, 'File.Position');
  Assert(Buf = '0123456789', 'File.Read');

  FH.Free;

  // FindFirst
  I := FS.FindFirst(Dir, FileName, [defUsed], [defUsed]);
  Assert(Assigned(I), 'FindFirst');
  Assert(not I.EOF, 'Iterator.EOF');
  Assert(I.Name = FileName, 'Iterator.Name');
  Assert(I.Size = 10, 'Iterator.Size');
  I.FindNext;
  Assert(I.EOF, 'Iterator.EOF');
  I.Free;

  // RenameFile
  Assert(FS.FileExists(Dir + FileName), 'FileExists');
  Assert(not FS.FileExists(Dir + 'rename.file'), 'FileExists');
  FS.RenameFile(Dir + FileName, 'rename.file');
  Assert(not FS.FileExists(Dir + FileName), 'FileExists');
  Assert(FS.FileExists(Dir + 'rename.file'), 'FileExists');
  FS.RenameFile(Dir + 'rename.file', FileName);
  Assert(FS.FileExists(Dir + FileName), 'FileExists');
  Assert(not FS.FileExists(Dir + 'rename.file'), 'FileExists');

  // DeleteFile
  Assert(FS.FileExists(Dir + FileName), 'FileExists');
  FS.DeleteFile(Dir + FileName);
  Assert(not FS.FileExists(Dir + FileName), 'FileExists');
  I := FS.FindFirst(Dir, FileName, [defUsed], [defUsed]);
  Assert(Assigned(I), 'FindFirst');
  Assert(I.EOF, 'Iterator.EOF');
  I.Free;
end;

procedure TestFileStress(const FS : TFileSystem; const Dir : RawByteString);
var FH  : TFSOpenFile;
    Buf : RawByteString;

  procedure TestSize(const Size : Integer);
  var Buf : Byte;
    begin
      FH.Size := Size;
      Assert(FH.Size = Size, 'File.Size');
      if Size = 0 then
        exit;
      FH.Position := Size - 1;
      Assert(FH.Position = Size - 1, 'File.Position');
      Buf := $DA;
      Assert(FH.Write(Buf, 1) = 1, 'File.Write');
      Assert(FH.Position = Size, 'File.Position');
      FH.Position := Size - 1;
      Assert(FH.Position = Size - 1, 'File.Position');
      Buf := $00;
      Assert(FH.Read(Buf, 1) = 1, 'File.Read');
      Assert(FH.Position = Size, 'File.Position');
      Assert(Buf = $DA, 'File.Read');
    end;

  procedure WriteBuf(const Size : Integer);
  var L : Integer;
    begin
      Buf := DupCharA('X', Size mod 16) +
             DupStrA('0123456789ABCDEF', Size div 16);
      FH.Size := 0;
      Assert(FH.Size = 0, 'File.Size');
      FH.Position := 0;
      Assert(FH.Position = 0, 'File.Position');
      L := Length(Buf);
      Assert(FH.Write(Pointer(Buf)^, L) = L, 'File.Write');
      Assert(FH.Size = L, 'File.Size');
      Assert(FH.Position = L, 'File.Position');
    end;

  procedure TestBufSize(const Size : Integer);
  var ReadBuf : RawByteString;
      L       : Integer;
    begin
      WriteBuf(Size);
      L := Length(Buf);
      FH.Position := 0;
      Assert(FH.Position = 0, 'File.Position');
      SetLength(ReadBuf, L);
      FillChar(Pointer(ReadBuf)^, L, #$A0);
      Assert(FH.Read(Pointer(ReadBuf)^, L) = L, 'File.Read');
      Assert(FH.Position = L, 'File.Position');
      Assert(ReadBuf = Buf, 'File.Read');
    end;

var I : Integer;

begin
  // File Create
  Assert(not FS.FileExists(Dir + 'stress.file'), 'FileExists');
  FH := FS.OpenFile(Dir + 'stress.file', fomCreate);
  Assert(FH.Position = 0, 'File.Position');
  Assert(FH.Size = 0, 'File.Size');

  // File size
  TestSize(1);
  TestSize(0);
  TestSize(FS_BlockSize);
  TestSize(0);
  TestSize(FS_BlockSize);
  TestSize(1);
  TestSize(FS_BlockSize - 1);
  TestSize(FS_BlockSize + 1);
  TestSize(1);
  TestSize(0);
  TestSize(FS_DataBytesPerAllocationEntry);
  TestSize(1);
  TestSize(FS_DataBytesPerAllocationEntry + 1);
  TestSize(1);
  TestSize(FS_DataBytesPerAllocationEntry + 1);
  TestSize(FS_DataBytesPerAllocationEntry - 1);
  TestSize(0);
  for I := 1 to FS_BlockSize + 1 do
    TestSize(I);
  for I := FS_BlockSize downto 0 do
    TestSize(I);
  for I := 0 to FS_BlockSize div 16 do
    begin
      TestSize(FS_BlockSize * 2 + I * 16);
      TestSize(I * 16);
    end;
  for I := 0 to FS_DataBlocksPerAllocationEntry * 4 do
    TestSize(I * FS_BlockSize);
  TestSize(0);
  for I := 1 to FS_DataBlocksPerAllocationEntry * 4 do
    TestSize(I * FS_BlockSize - 1);
  TestSize(0);
  for I := 0 to FS_DataBlocksPerAllocationEntry * 4 do
    TestSize(I * FS_BlockSize + 1);
  for I := 0 to 7 do
    begin
      TestSize(I);
      TestSize(FS_BlockSize + I);
      TestSize(FS_BlockSize * 4 - I);
      TestSize(FS_BlockSize * I);
      TestSize(I * 3);
      TestSize(FS_DataBytesPerAllocationEntry + I);
      TestSize(I + 3);
      TestSize(FS_DataBytesPerAllocationEntry * I);
    end;

  // Read/Write
  TestBufSize(1);
  TestBufSize(2);
  TestBufSize(FS_BlockSize);
  TestBufSize(FS_BlockSize - 1);
  TestBufSize(FS_BlockSize + 1);
  TestBufSize(FS_DataBytesPerAllocationEntry);
  TestBufSize(FS_DataBytesPerAllocationEntry - 1);
  TestBufSize(FS_DataBytesPerAllocationEntry + 1);
  for I := 1 to FS_DataBlocksPerAllocationEntry * 4 do
    TestBufSize(FS_BlockSize * I);
  for I := -8 to 8 do
    begin
      TestBufSize(FS_BlockSize + I);
      TestBufSize(FS_DataBytesPerAllocationEntry + I);
    end;
  for I := 1 to 8 do
    begin
      TestBufSize(I);
      TestBufSize(FS_DataBytesPerAllocationEntry + I);
    end;

  FH.Free;

  // Delete File
  Assert(FS.FileExists(Dir + 'stress.file'), 'FileExists');
  FS.DeleteFile(Dir + 'stress.file');
  Assert(not FS.FileExists(Dir + 'stress.file'), 'FileExists');
end;

procedure TestDirStress(const FS : TFileSystem; const Dir : RawByteString);
var I : Integer;
    S : RawByteString;
    F : Array [0..FS_DirectoryEntriesPerBlock * 4] of Boolean;
    T : TFSIterator;
begin
  // Multiple directories
  Assert(FS.DirectoryExists(Dir), 'DirectoryExists');
  for I := 0 to FS_DirectoryEntriesPerBlock * 4 do
    FS.MakeDirectory(Dir + 'Dir' + IntToStringA(I));
  for I := 0 to FS_DirectoryEntriesPerBlock * 4 do
    begin
      S := Dir + 'Dir' + IntToStringA(I) + '/';
      Assert(FS.DirectoryExists(S), 'DirectoryExists');
      TestFileOps(FS, S, 'test.file');
      TestDirOps(FS, S);
      F [I] := False;
    end;
  TestFileOps(FS, Dir, 'test.file');
  T := FS.FindFirst(Dir, 'Dir*', [], []);
  Assert(Assigned(T), 'FindFirst');
  While not T.EOF do
    begin
      if StrMatchLeftB(T.Name, 'Dir', True) then
        begin
          Assert(defDirectory in T.Flags, 'Iterator.Flags');
          I := StringToIntDefA(CopyFromB(T.Name, 4), -1);
          Assert(I >= 0, 'Iterator');
          F [I] := True;
        end;
      T.FindNext;
    end;
  T.Free;
  for I := 0 to FS_DirectoryEntriesPerBlock * 4 do
    Assert(F [I], 'FindNext');
  for I := 0 to FS_DirectoryEntriesPerBlock * 4 do
    FS.RemoveDirectory(Dir + 'Dir' + IntToStringA(I));
  for I := 0 to FS_DirectoryEntriesPerBlock * 4 do
    Assert(not FS.DirectoryExists(Dir + 'Dir' + IntToStringA(I)), 'DirectoryExists');
  T := FS.FindFirst(Dir, 'Dir*', [defDirectory], [defDirectory]);
  Assert(Assigned(T), 'FindFirst');
  Assert(T.EOF, 'Iterator.EOF');
  T.Free;

  // Deep directories
  S := Dir;
  for I := 0 to 31 do
    begin
      S := S + Word32ToHexA(I, 8) + '/';
      FS.MakeDirectory(S);
      Assert(FS.DirectoryExists(S), 'DirectoryExists');
      TestFileOps(FS, S, 'test.file');
      TestDirOps(FS, S);
    end;
  TestFileOps(FS, Dir, 'test.file');
  for I := 0 to 31 do
    begin
      FS.RemoveDirectory(S);
      Assert(not FS.DirectoryExists(S), 'DirectoryExists');
      SetLength(S, Length(S) - 9);
    end;
end;

procedure TestDir(const FS : TFileSystem; const Dir : RawByteString);
begin
  TestDirOps(FS, Dir);
  TestFileOps(FS, Dir, 'test.file');
  TestDirStress(FS, Dir);
  TestFileOps(FS, Dir, DupCharA('A', FS_MaxFileNameLength));
  TestFileStress(FS, Dir);
  TestFileOps(FS, Dir, '1');
end;

procedure TestFileSystem(const FS : TFileSystem; const Password : RawByteString);
begin
  // Format
  Assert(not FS.IsOpen, 'IsOpen');
  FS.Format(Password, fseNone);
  Assert(not FS.IsOpen, 'IsOpen');

  // Open
  FS.Open(Password);
  Assert(FS.IsOpen, 'IsOpen');

  // DirectoryExists
  Assert(FS.DirectoryExists('/'), 'DirectoryExists');
  Assert(not FS.DirectoryExists('test'), 'DirectoryExists');
  Assert(not FS.DirectoryExists('test/'), 'DirectoryExists');

  // Test root directory
  TestDir(FS, '/');

  // Test sub directory
  FS.MakeDirectory('/tst');
  TestDir(FS, '/tst/');
  FS.RemoveDirectory('/tst');
  Assert(not FS.DirectoryExists('/tst'), 'DirectoryExists');

  // Close
  Assert(FS.IsOpen, 'IsOpen');
  FS.Close;
  Assert(not FS.IsOpen, 'IsOpen');

  // Re-open
  FS.Open(Password);
  Assert(FS.IsOpen, 'IsOpen');
  TestFileOps(FS, '/', 'test.file');
  TestDirOps(FS, '/');
  FS.Close;
  Assert(not FS.IsOpen, 'IsOpen');
end;

procedure Test;
var FS : TFileSystem;
    Fi : TFileStream;
begin
  TestStructs;

  Fi := TFileStream.Create('c:\temp\test.fs', fsomCreateIfNotExist);
  Fi.Size := 10 * 1024 * 1024;
  FS := TFileSystem.CreateEx(Fi, True);
  TestFileSystem(FS, '');
  Fi.DeleteFile;
  FS.Free;

  Fi := TFileStream.Create('c:\temp\test.fs', fsomCreateIfNotExist);
  Fi.Size := 16 * 1024;
  FS := TFileSystem.CreateEx(Fi, True);
  TestFileSystem(FS, 'Password');
  Fi.DeleteFile;
  FS.Free;
end;

procedure ProfileFile(const FS: TFileSystem);
var I : Integer;
    N : LongWord;
begin
  FS.Format;
  FS.Open;

  N := GetTick;
  for I := 1 to 1000 do
    FS.OpenFile(IntToStringA(I), fomCreate).Free;
  N := TickDelta(N, GetTick);
  Writeln('FS Create: ', N / 1000:0:3, 'ms');

  N := GetTick;
  for I := 1 to 1000 do
    TFileStream.Create('c:\temp\' + IntToStr(I), fsomCreate, [], fsahNone).Free;
  N := TickDelta(N, GetTick);
  Writeln('OS Create: ', N / 1000:0:3, 'ms');
  for I := 1 to 1000 do
    FileDeleteEx('c:\temp\' + IntToStr(I));

  FS.Close;
end;

procedure Profile;
var FS : TFileSystem;
    Fi : TFileStream;
begin
  Fi := TFileStream.Create('c:\temp\test.fs', fsomCreateIfNotExist);
  Fi.Size := 10 * 1024 * 1024;
  FS := TFileSystem.CreateEx(Fi, True);
  ProfileFile(FS);
  Fi.DeleteFile;
  FS.Free;
end;



end.


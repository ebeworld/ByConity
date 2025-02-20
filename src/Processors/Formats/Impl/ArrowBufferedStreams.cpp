#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Storages/HDFS/ReadBufferFromByteHDFS.h>
#include <arrow/util/future.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>

#include <sys/stat.h>


namespace DB
{

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowBufferedOutputStream::Tell() const
{
    return arrow::Result<int64_t>(total_length);
}

arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_)
    : in{in_}, file_size{file_size_}, is_open{true}
{
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    return arrow::Result<int64_t>(file_size);
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return in.getPosition();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    in.seek(position, SEEK_SET);
    return arrow::Status::OK();
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status ArrowInputStreamFromReadBuffer::Abort()
{
    return arrow::Status();
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Tell() const
{
    return in.count();
}

arrow::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow::Status();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(ReadBuffer & in)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }

    if (auto * fd_in = dynamic_cast<ReadBufferFromByteHDFS *>(&in))
    {
        size_t file_size = fd_in->getFileSize();
        return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, file_size);
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

RandomAccessFileFromRandomAccessReadBuffer::RandomAccessFileFromRandomAccessReadBuffer(SeekableReadBuffer & in_, size_t file_size_) : in(in_), file_size(file_size_) {}

arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::GetSize()
{
    return file_size;
}

arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    return in.readBigAt(reinterpret_cast<char*>(out), nbytes, position);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::ReadAt(int64_t position, int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(position, nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Future<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::ReadAsync(const arrow::io::IOContext&, int64_t position, int64_t nbytes)
{
    return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(ReadAt(position, nbytes));
}

arrow::Status RandomAccessFileFromRandomAccessReadBuffer::Close()
{
    chassert(is_open);
    is_open = false;
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromRandomAccessReadBuffer::Seek(int64_t) { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::Tell() const { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::Read(int64_t, void*) { return arrow::Status::NotImplemented(""); }
arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::Read(int64_t) { return arrow::Status::NotImplemented(""); }

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(SeekableReadBuffer & in, size_t file_size)
{
    if (in.supportsReadAt())
        return std::make_shared<RandomAccessFileFromRandomAccessReadBuffer>(in, file_size);
    else
        return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(in, file_size);
}

}

#endif

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/FileSystemUtils.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <regex>
#include <dirent.h>

bool parseS3Uri(const std::string& uri, std::string& bucket, std::string& prefix, std::string& objectKey) {
    const std::string prefix_str = "s3://";
    if (uri.substr(0, prefix_str.length()) != prefix_str) return false;

    std::string rest = uri.substr(prefix_str.length());
    size_t firstSlash = rest.find('/');

    if (firstSlash == std::string::npos) {
        // No object path given
        bucket = rest;
        prefix.clear();
        objectKey.clear();
        return true;
    }

    bucket = rest.substr(0, firstSlash);
    std::string keyPath = rest.substr(firstSlash + 1);  // everything after bucket/

    size_t lastSlash = keyPath.find_last_of('/');
    if (lastSlash == std::string::npos) {
        // No prefix, only object
        prefix.clear();
        objectKey = keyPath;
    } else {
        prefix = keyPath.substr(0, lastSlash);
        objectKey = keyPath.substr(lastSlash + 1);
    }

    return true;
}

constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t BUF_SIZE = 512 * 1024;  // 2MB

class DirectIOStreamBuf : public std::streambuf {
public:
    DirectIOStreamBuf(const std::string& path)
        : fd_(-1), file_size_(0), bytes_read_(0) {

        fd_ = open(path.c_str(), O_RDONLY | O_DIRECT);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open file with O_DIRECT");
        }

        struct stat st;
        if (fstat(fd_, &st) != 0) {
            throw std::runtime_error("Failed to stat file");
        }
        file_size_ = st.st_size;

        // Allocate aligned buffer
        if (posix_memalign((void**)&buffer_, BLOCK_SIZE, BUF_SIZE) != 0) {
            throw std::runtime_error("Failed to allocate aligned buffer");
        }

        memset(buffer_, 0, BUF_SIZE);
        setg(buffer_, buffer_, buffer_);
    }

    ~DirectIOStreamBuf() {
        if (fd_ >= 0) close(fd_);
        free(buffer_);
    }

protected:
    int_type underflow() override {
        if (bytes_read_ >= file_size_) {
            return traits_type::eof();
        }

        ssize_t bytes = read(fd_, buffer_, BUF_SIZE);
        if (bytes <= 0) {
            return traits_type::eof();
        }

        bytes_read_ += bytes;
        setg(buffer_, buffer_, buffer_ + bytes);
        return traits_type::to_int_type(*gptr());
    }

private:
    int fd_;
    size_t file_size_;
    size_t bytes_read_;
    char* buffer_;
};

class DirectIOStream : public std::iostream {
public:
    DirectIOStream(const std::string& path)
        : std::iostream(nullptr), buf_(path) {
        rdbuf(&buf_);
    }

private:
    DirectIOStreamBuf buf_;
};

// Upload file to S3
int UploadFile(const std::string& bucket, const std::string& object_key, const std::string& file_path, Aws::S3::S3Client& client) {
    struct stat st;
    if (stat(file_path.c_str(), &st) != 0) {
        std::cerr << "File stat failed\n";
        return 1;
    }

    //std::cout << " Bucket: "<< bucket << " Key: " << object_key << " file path: " << file_path << std::endl;
    auto stream = Aws::MakeShared<DirectIOStream>("UploadTag", file_path);

    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object_key);
    request.SetBody(stream);
    request.SetContentLength(static_cast<long>(st.st_size));

    auto outcome = client.PutObject(request);
    if (outcome.IsSuccess()) {
            //std::cout << outcome.GetResult().GetETag() << std::endl;
    } else {
            std::cerr << "Upload failed: " << outcome.GetError().GetMessage() << std::endl;
            return 1;
    }
    return 0;
}

bool make_parent_dirs(const std::string& path) {
    size_t pos = 0;
    while ((pos = path.find('/', pos + 1)) != std::string::npos) {
        std::string dir = path.substr(0, pos);
        if (!dir.empty()) {
            mkdir(dir.c_str(), 0755);
        }
    }
    return true;
}

void* aligned_alloc_block(size_t alignment, size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) return nullptr;
    return ptr;
}

#if 0
// Download S3 object to file
int DownloadFile(const Aws::S3::S3Client& client, const std::string& bucket, const Aws::String& key, const std::string& target_base) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key);

    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed to download: " << key << " - " << outcome.GetError().GetMessage() << "\n";
        return 1;
    }

    std::string full_path = target_base;
    make_parent_dirs(full_path);

    int fd = open(full_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        std::cerr << "Failed to open " << full_path << ": " << strerror(errno) << "\n";
        return 1;
    }

    constexpr size_t BLOCK_SIZE = 4096;
    constexpr size_t CHUNK_SIZE = 2 * 1024 * 1024;  // 2MB
    size_t aligned_chunk_size = ((CHUNK_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;

    std::unique_ptr<char, decltype(&free)> aligned_buf(
        static_cast<char*>(aligned_alloc_block(BLOCK_SIZE, aligned_chunk_size)), &free
    );

    if (!aligned_buf) {
        std::cerr << "Failed to allocate aligned 2MB buffer\n";
        close(fd);
        return 1;
    }

    auto& stream = outcome.GetResult().GetBody();
    size_t total = 0;
    off_t offset = 0;
    while (true) {
        stream.read(aligned_buf.get(), CHUNK_SIZE);
        std::streamsize bytes_read = stream.gcount();
        if (bytes_read <= 0) break;

        size_t padded = ((bytes_read + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        if ((size_t)bytes_read < padded) {
            memset(aligned_buf.get() + bytes_read, 0, padded - bytes_read);
        }

        if (lseek(fd, offset, SEEK_SET) == -1) {
            std::cerr << "lseek failed: " << strerror(errno) << "\n";
            close(fd);
            return 1;
        }

        ssize_t written = write(fd, aligned_buf.get(), padded);
        if (written < 0) {
            std::cerr << "write failed: " << strerror(errno) << "\n";
            close(fd);
            return 1;
        }

        offset += written;
        total += written;
    }


    close(fd);
    return 0;
}
#endif
int DownloadFile(const Aws::S3::S3Client& client, const std::string& bucket, const Aws::String& key, const std::string& target_base) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key);

    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed to download: " << key << " - " << outcome.GetError().GetMessage() << "\n";
        return 1;
    }

    std::string full_path = target_base;
    make_parent_dirs(full_path);

    std::ofstream output(full_path, std::ios::binary | std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        std::cerr << "Failed to open " << full_path << " for writing\n";
        return 1;
    }

    constexpr size_t CHUNK_SIZE = 2 * 1024 * 1024;  // 2MB
    std::vector<char> buffer(CHUNK_SIZE);

    auto& stream = outcome.GetResult().GetBody();
    while (stream) {
        stream.read(buffer.data(), CHUNK_SIZE);
        std::streamsize bytes_read = stream.gcount();
        if (bytes_read > 0) {
            output.write(buffer.data(), bytes_read);
            if (!output) {
                std::cerr << "Failed to write to " << full_path << "\n";
                return 1;
            }
        }
    }

    output.close();
    return 0;
}

bool file_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

int main(int argc, char** argv) {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        std::string endpointUrl;
        std::vector<std::string> positionalArgs;

	for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--endpoint-url" && i + 1 < argc) {
                endpointUrl = argv[++i];
            } else if (arg.rfind("-", 0) != 0) {
                // Only accept non-option args as positional args
                positionalArgs.push_back(arg);
                if (positionalArgs.size() >= 2) break; // Stop collecting after src and dst
            }
        }

	if (positionalArgs.size() != 2) {
            std::cerr << "Usage:\n"
                      << "  cloudcp [--endpoint-url <url>] /path/to/file s3://bucket[/prefix/]\n"
                      << "  cloudcp [--endpoint-url <url>] s3://bucket[/prefix/] /path/to/file\n";
            Aws::ShutdownAPI(options);
            return 1;
        }

        std::string src = positionalArgs[0];
        std::string dst = positionalArgs[1];

        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true; // Disable only if using self-signed certs
        config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
        config.connectTimeoutMs = 10000;   // connection timeout
        if (!endpointUrl.empty()) {
            config.endpointOverride = endpointUrl.c_str();
        }
        const std::string ca_cert_path = "/usr/local/share/ca-certificates/minio.crt";
        if (file_exists(ca_cert_path)) {
            config.caFile = ca_cert_path;
        }

        Aws::S3::S3Client s3(config);
	std::string bucket, prefix, objectKey;

        if (parseS3Uri(src, bucket, prefix, objectKey)) {
            // Download
            std::string localPath = dst;
            std::string key = prefix.empty() ? objectKey : (prefix + "/" + objectKey);
            if (!DownloadFile(s3, bucket, key, localPath)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
        } else if (parseS3Uri(dst, bucket, prefix, objectKey)) {
            // Upload
            std::string localPath = src;
            std::string filename = localPath.substr(localPath.find_last_of("/\\") + 1);
            std::string key = prefix.empty() ? filename : (prefix + "/" + filename);
            if (!UploadFile(bucket, key, localPath, s3)) {
                Aws::ShutdownAPI(options);
                return 1;
            }
        } else {
            std::cerr << "Invalid source or destination. One must be s3://bucket/key\n";
            Aws::ShutdownAPI(options);
            return 1;
        }
    }
    Aws::ShutdownAPI(options);
    return 0;
}

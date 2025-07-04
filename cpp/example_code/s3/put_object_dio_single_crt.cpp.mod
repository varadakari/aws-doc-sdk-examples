#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iostream>
#include <memory>
#include <vector>
#include <streambuf>

constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t BUF_SIZE = 2 * 1024 * 1024;  // 2MB
//constexpr size_t BUF_SIZE = 512 * 1024;  // 2MB

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

void upload_file_directio(const std::string& bucket, const std::string& object_key, const std::string& file_path, Aws::S3Crt::S3CrtClient& client) {
    struct stat st;
    if (stat(file_path.c_str(), &st) != 0) {
        std::cerr << "File stat failed\n";
        return;
    }

    auto stream = Aws::MakeShared<DirectIOStream>("UploadTag", file_path);

    Aws::S3Crt::Model::PutObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(object_key);
    request.SetBody(stream);
    request.SetContentLength(static_cast<long>(st.st_size));

    auto outcome = client.PutObject(request);
    if (outcome.IsSuccess()) {
        std::cout << "Upload succeeded: " << object_key << std::endl;
    } else {
        std::cerr << "Upload failed: " << outcome.GetError().GetMessage() << std::endl;
    }
}

int main(int argc, char **argv) {
    if( argc < 4 ) {
        printf("%s <bucket name> <key> <filename>\n", argv[0]);
        exit(1);
    }
    Aws::String bucket = argv[1];
    Aws::String object_key = argv[2];
    Aws::String file_path = argv[3];
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
#if 0
        Aws::Client::ClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = false;
	config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
	config.connectTimeoutMs = 10000;   // connection timeout
	config.httpLibOverride = Aws::Http::TransferLibType::CURL_CLIENT;
	// below id for local minio
        config.endpointOverride = "https://10.10.10.155:9000";
        config.region = "us-east-1";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";
	Aws::S3::S3Client s3 = Aws::S3::S3Client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config,
		       		Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
#endif

        Aws::S3Crt::S3CrtClientConfiguration clientConfig;
	clientConfig.region = "us-west-1";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // use with caution
        clientConfig.useVirtualAddressing = false; // for non-AWS S3
        clientConfig.caFile = "/home/ubuntu/varada/minio.crt";
	clientConfig.enableTcpKeepAlive = true;
        clientConfig.tcpKeepAliveIntervalMs = 30000;
	//clientConfig.throughputTargetGbps = 100.0;
        //clientConfig.connectTimeoutMs = 3000;
        //clientConfig.requestTimeoutMs = 5000;
        clientConfig.endpointOverride = "https://10.10.10.155:9000";

	Aws::Auth::AWSCredentials creds("minioadmin", "minioadmin"); // or your MinIO access key & secret
								     //
								     //
        //if AWS
	//auto credentialsProvider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("S3Upload");
        //Aws::S3Crt::S3CrtClient s3crt_client(credentialsProvider, clientConfig);
	//else
	Aws::S3Crt::S3CrtClient s3crt_client(creds, clientConfig);

        upload_file_directio(bucket, object_key, file_path, s3crt_client);
    }
    Aws::ShutdownAPI(options);
    return 0;
}


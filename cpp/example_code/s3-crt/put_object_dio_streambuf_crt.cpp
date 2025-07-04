#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <streambuf>
#include <memory>

constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t ALIGNMENT = 4096;
constexpr size_t MAX_THREADS = 4;

class DirectIOStreamBuf : public std::streambuf {
public:
    DirectIOStreamBuf(const std::string& path, size_t chunk_size = 2 *1024 * 1024)
        : fd_(-1), file_size_(0), read_offset_(0) {

        // Align chunk size to 4096
        chunk_size_ = ((chunk_size + 4095) / 4096) * 4096;

        if (posix_memalign(reinterpret_cast<void**>(&buffer_), 4096, chunk_size_) != 0) {
            throw std::runtime_error("Failed to allocate aligned buffer");
        }

        fd_ = open(path.c_str(), O_RDONLY | O_DIRECT);
        if (fd_ < 0) {
            free(buffer_);
            throw std::runtime_error("Failed to open file: " + path);
        }

        struct stat st;
        if (fstat(fd_, &st) == 0) {
            file_size_ = st.st_size;
        }

        setg(buffer_, buffer_ + chunk_size_, buffer_ + chunk_size_); // initially empty
    }

    ~DirectIOStreamBuf() override {
        if (fd_ >= 0) close(fd_);
        free(buffer_);
    }

    size_t GetFileSize() const { return file_size_; }

protected:
    int_type underflow() override {
        if (read_offset_ >= file_size_) {
            return traits_type::eof();
        }

        size_t to_read = std::min(chunk_size_, file_size_ - read_offset_);
        size_t aligned_read = ((to_read + 4095) / 4096) * 4096;
        std::memset(buffer_, 0, aligned_read);

        ssize_t bytes_read = pread(fd_, buffer_, aligned_read, read_offset_);
        if (bytes_read < 0) {
            perror("pread failed");
            return traits_type::eof();
        }

        read_offset_ += to_read;
        setg(buffer_, buffer_, buffer_ + to_read);
        return traits_type::to_int_type(*gptr());
    }

private:
    int fd_;
    size_t file_size_;
    size_t read_offset_;
    size_t chunk_size_;
    char* buffer_;
};

class DirectIOStream : public Aws::IOStream {
public:
    DirectIOStream(const std::string& path, size_t chunk_size = 2 * 1024 * 1024)
        : Aws::IOStream(nullptr), buf_(path, chunk_size) {
        this->rdbuf(&buf_);
    }

    size_t GetFileSize() const {
        return buf_.GetFileSize();
    }

private:
    DirectIOStreamBuf buf_;
};


class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers.push_back(std::thread([this]() {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(this->mutex_);
                        this->condition_.wait(lock, [this]() {
                            return this->stop || !this->tasks_.empty();
                        });

                        if (this->stop && this->tasks_.empty())
                            return;

                        task = std::move(this->tasks_.front());
                        this->tasks_.pop();
                    }

                    task();
                }
            }));
        }
    }

    void enqueue(const std::function<void()>& task) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            tasks_.push(task);
        }
        condition_.notify_one();
    }

    void shutdown() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            stop = true;
        }
        condition_.notify_all();

        for (size_t i = 0; i < workers.size(); ++i) {
            if (workers[i].joinable())
                workers[i].join();
        }
    }

    ~ThreadPool() {
        shutdown();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()> > tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
    bool stop;
};

void upload_file_stream(const Aws::String& bucket, const std::string& path, const std::string& filename, std::shared_ptr<Aws::S3Crt::S3CrtClient> s3) {
    std::string full_path = path + "/" + filename;
    struct stat st;
    if (stat(full_path.c_str(), &st) != 0) {
        std::cerr << "File stat failed for: " << full_path << std::endl;
        return;
    }

    size_t file_size = st.st_size;

    try {
	auto stream = Aws::MakeShared<DirectIOStream>("UploadTag", full_path);

        Aws::S3Crt::Model::PutObjectRequest request;
        request.SetBucket(bucket);
        request.SetKey(filename);
        request.SetBody(stream);
        request.SetContentLength(static_cast<long>(file_size));

        auto outcome = s3->PutObject(request);
        if (outcome.IsSuccess()) {
            std::cout << "Streamed upload complete: " << filename << std::endl;
        } else {
            std::cerr << "Upload failed: " << outcome.GetError().GetMessage() << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Stream setup failed: " << e.what() << std::endl;
    }
}

// Traverse directory and dispatch uploads
void process_directory(const std::string& dir_path, const Aws::String& bucket, std::shared_ptr<Aws::S3Crt::S3CrtClient> s3, ThreadPool& pool) {
    DIR* dir = opendir(dir_path.c_str());
    if (!dir) {
        perror(("opendir " + dir_path).c_str());
        return;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != NULL) {
        std::string name = entry->d_name;
        if (name == "." || name == "..")
            continue;

        pool.enqueue([bucket, dir_path, name, &s3]() {
            upload_file_stream(bucket, dir_path, name, s3);
        });
    }

    closedir(dir);
}

int main(int argc, char **argv) {
    if( argc < 4 ) {
        printf("%s <bucket name> <dir> <num_threads>\n", argv[0]);
        exit(1);
    }

    Aws::String bucket_name = argv[1];
    Aws::String dir_name = argv[2];
    int num_threads = std::stoi(argv[3]);
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::S3Crt::S3CrtClientConfiguration config;
        config.scheme = Aws::Http::Scheme::HTTPS;
        //config.verifySSL = false;
        config.endpointOverride = "https://10.10.10.155:9000";
        config.region = "us-east-1";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";
	config.requestTimeoutMs = 60000;   // 30 seconds or higher as needed
	config.connectTimeoutMs = 10000;   // connection timeout
        config.verifySSL = true; // use with caution
        config.useVirtualAddressing = false; // for non-AWS S3
        config.caFile = "/home/ubuntu/varada/minio.crt";
	config.enableTcpKeepAlive = true;
        config.tcpKeepAliveIntervalMs = 30000;
	config.throughputTargetGbps = 100.0;
	//config.memoryLimitBytes = (8 * 1024 * 1024 * 1024); // 8GB
	config.multipartUploadThreshold = 8; // 8MB
	config.maxConnections = 128;

	Aws::Auth::AWSCredentials creds("minioadmin", "minioadmin"); // or your MinIO access key & secret

        //Aws::S3::S3Client s3_client(config);
        //Aws::S3::S3Client* s3_client = new Aws::S3::S3Client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config, signPayloads, false);
	//std::shared_ptr<Aws::S3::S3Client> s3_client = std::make_shared<Aws::S3::S3Client>(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()), config, signPayloads, false);
	std::shared_ptr<Aws::S3Crt::S3CrtClient> s3_client = std::make_shared<Aws::S3Crt::S3CrtClient>(creds, config);

        ThreadPool pool(num_threads);

        process_directory(dir_name , bucket_name , s3_client, pool);

        pool.shutdown();  // Wait for all uploads to finish
    }
    Aws::ShutdownAPI(options);
    return 0;
}


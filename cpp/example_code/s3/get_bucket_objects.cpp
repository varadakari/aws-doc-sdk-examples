#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/LogMacros.h>

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <dirent.h>

constexpr size_t BLOCK_SIZE = 4096;
constexpr size_t CHUNK_SIZE = 2 * 1024 * 1024; // 2MB

// Aligned allocation
void* aligned_alloc_block(size_t alignment, size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
}

// Create directories for output path
void make_parent_dirs(const std::string& path) {
    size_t pos = 0;
    while ((pos = path.find('/', pos + 1)) != std::string::npos) {
        std::string dir = path.substr(0, pos);
        mkdir(dir.c_str(), 0755); // Ignore errors
    }
}

// Thread-safe task queue
class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]() {
                            return this->stop || !this->tasks.empty();
                        });
                        if (this->stop && this->tasks.empty())
                            return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }

                    task();
                }
            });
        }
    }

    void enqueue(std::function<void()> task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.push(std::move(task));
        }
        condition.notify_one();
    }

    void shutdown() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

    ~ThreadPool() {
        shutdown();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()> > tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
};

// Download a single S3 object using Direct I/O
bool download_object_direct_io(const Aws::S3::S3Client& client, const std::string& bucket,
                               const Aws::String& key, const std::string& target_dir) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket.c_str());
    request.SetKey(key);

    auto outcome = client.GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "Failed: " << key << " - " << outcome.GetError().GetMessage() << "\n";
        return false;
    }

    std::string full_path = target_dir + "/" + key.c_str();
    make_parent_dirs(full_path);

    int fd = open(full_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        std::cerr << "Open failed: " << full_path << ": " << strerror(errno) << "\n";
        return false;
    }

    std::unique_ptr<char, decltype(&free)> buffer(
        static_cast<char*>(aligned_alloc_block(BLOCK_SIZE, CHUNK_SIZE)), &free
    );

    if (!buffer) {
        std::cerr << "Aligned buffer alloc failed\n";
        close(fd);
        return false;
    }

    auto& stream = outcome.GetResult().GetBody();
    while (stream.good()) {
        stream.read(buffer.get(), CHUNK_SIZE);
        std::streamsize bytes_read = stream.gcount();
        if (bytes_read <= 0) break;

        size_t padded_size = ((bytes_read + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        if (static_cast<size_t>(bytes_read) < padded_size) {
            std::memset(buffer.get() + bytes_read, 0, padded_size - bytes_read);
        }

        ssize_t written = write(fd, buffer.get(), padded_size);
        if (written < 0) {
            std::cerr << "Write error for " << key << ": " << strerror(errno) << "\n";
            close(fd);
            return false;
        }
    }

    close(fd);
    return true;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ./s3_downloader <bucket> <target_directory>\n";
        return 1;
    }

    std::string bucket = argv[1];
    std::string target_dir = argv[2];

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-east-1";
        clientConfig.endpointOverride = "https://10.10.10.155:9000";
        clientConfig.scheme = Aws::Http::Scheme::HTTPS;
        clientConfig.verifySSL = true; // Disable only if using self-signed certs
        clientConfig.caFile = "/home/ubuntu/varada/minio.crt";
	const Aws::String access_key_ = "minioadmin";
        const Aws::String secret_key_ = "minioadmin";

        
	Aws::S3::S3Client client(Aws::Auth::AWSCredentials(access_key_.data(), secret_key_.data()),
			 	    clientConfig,
                                    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                    false);

        Aws::S3::Model::ListObjectsV2Request list_request;
        list_request.SetBucket(bucket.c_str());

        ThreadPool pool(std::thread::hardware_concurrency());

        Aws::String continuation_token;
        do {
            if (!continuation_token.empty()) {
                list_request.SetContinuationToken(continuation_token);
	    }
            auto list_outcome = client.ListObjectsV2(list_request);
            if (!list_outcome.IsSuccess()) {
                std::cerr << "ListObjects failed: " << list_outcome.GetError().GetMessage() << "\n";
                break;
            }

            const auto& objects = list_outcome.GetResult().GetContents();
            for (const auto& object : objects) {
                Aws::String key = object.GetKey();
                pool.enqueue([bucket, key, &client, target_dir]() {
                    download_object_direct_io(client, bucket, key, target_dir);
                });
            }

            continuation_token = list_outcome.GetResult().GetNextContinuationToken();
        } while (!continuation_token.empty());
    }
    Aws::ShutdownAPI(options);
    return 0;
}

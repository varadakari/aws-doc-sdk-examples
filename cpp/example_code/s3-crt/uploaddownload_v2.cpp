#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/CreateMultipartUploadRequest.h>
#include <aws/s3-crt/model/UploadPartRequest.h>
#include <aws/s3-crt/model/CompleteMultipartUploadRequest.h>
#include <aws/s3-crt/model/PutObjectRequest.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/HeadObjectRequest.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/Outcome.h>
#include <fstream>
#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

using namespace Aws;
using namespace Aws::S3Crt;
using namespace Aws::S3Crt::Model;

//static const size_t PART_SIZE = 16 * 1024 * 1024; // 16 MB parts
static const size_t MULTIPART_THRESHOLD = 512 * 1024 * 1024; // 512 MB

// Get file size
uint64_t GetFileSize(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    return file ? static_cast<uint64_t>(file.tellg()) : 0;
}


bool UploadSingle(std::shared_ptr<S3CrtClient> client, const std::string& bucket, const std::string& key, const std::string& filePath) {
    PutObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    auto input_data = Aws::MakeShared<Aws::FStream>("UploadStream", filePath.c_str(), std::ios::binary | std::ios::in);
    if (!input_data->good()) {
        std::cerr << "Failed to open file: " << filePath << std::endl;
        return false;
    }
    request.SetBody(input_data);

    auto outcome = client->PutObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "PutObject failed: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    return true;
}

#define PART_SIZE (64 * 1024 * 1024) // 64MB chunks
#define CHUNK_SIZE (4 * 1024 * 1024) // 4MB read chunks
#define ALIGNMENT 4096

struct PartETag {
    int partNumber;
    Aws::String eTag;
};

struct UploadChunk {
    int partNumber;
    size_t size;
    char* data;
};

class ThreadSafeQueue {
    std::queue<UploadChunk> queue;
    std::mutex mtx;
    std::condition_variable cv;
    bool done = false;

public:
    void push(UploadChunk&& chunk) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            queue.push(std::move(chunk));
        }
        cv.notify_one();
    }

    bool pop(UploadChunk& result) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return !queue.empty() || done; });
        if (queue.empty()) return false;
        result = std::move(queue.front());
        queue.pop();
        return true;
    }

    void set_done() {
        {
            std::lock_guard<std::mutex> lock(mtx);
            done = true;
        }
        cv.notify_all();
    }
};

class FixedBufStreamBuf : public std::streambuf {
public:
    FixedBufStreamBuf(char* base, size_t len) {
        setg(base, base, base + len);
        setp(base, base + len);
    }
};

class FixedBufIStream : public Aws::IOStream {
    FixedBufStreamBuf buffer;
public:
    FixedBufIStream(char* base, size_t len)
        : Aws::IOStream(&buffer), buffer(base, len) {}
};


bool CompleteMultipartUpload(const std::shared_ptr<S3CrtClient>& client, const std::string& bucket, const std::string& key, const std::string& uploadId, std::vector<PartETag>& completedParts) {
    std::sort(completedParts.begin(), completedParts.end(), [](const PartETag& a, const PartETag& b) {
        return a.partNumber < b.partNumber;
    });

    CompletedMultipartUpload completedUpload;
    Aws::Vector<CompletedPart> awsParts;
    for (const auto& part : completedParts) {
        CompletedPart completed;
        completed.SetETag(part.eTag);
        completed.SetPartNumber(part.partNumber);
        awsParts.push_back(std::move(completed));
    }
    completedUpload.SetParts(std::move(awsParts));

    CompleteMultipartUploadRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);
    request.SetUploadId(uploadId);
    request.SetMultipartUpload(std::move(completedUpload));

    auto outcome = client->CompleteMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "CompleteMultipartUpload failed: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }
    return true;
}


bool DoMultipartUpload(std::shared_ptr<S3CrtClient> client,
                       const std::string& bucket,
                       const std::string& key,
                       const std::string& filePath,
                       size_t threadCount)
{
    CreateMultipartUploadRequest createRequest;
    createRequest.SetBucket(bucket);
    createRequest.SetKey(key);
    auto createOutcome = client->CreateMultipartUpload(createRequest);
    if (!createOutcome.IsSuccess()) {
        std::cerr << "CreateMultipartUpload failed: " << createOutcome.GetError().GetMessage() << std::endl;
        return false;
    }
    const std::string uploadId = createOutcome.GetResult().GetUploadId();

    ThreadSafeQueue queue;
    std::vector<std::thread> workers;
    std::vector<PartETag> completedParts;
    std::mutex completedMtx;
    std::atomic<size_t> uploadedBytes(0);

    for (size_t i = 0; i < threadCount; ++i) {
        workers.emplace_back([&]() {
            UploadChunk chunk;
            while (queue.pop(chunk)) {
                UploadPartRequest request;
                request.SetBucket(bucket);
                request.SetKey(key);
                request.SetUploadId(uploadId);
                request.SetPartNumber(chunk.partNumber);

                auto stream = Aws::MakeShared<FixedBufIStream>("UploadStream", chunk.data, chunk.size);
                request.SetBody(stream);
                request.SetContentLength(chunk.size);

                auto outcome = client->UploadPart(request);
                if (outcome.IsSuccess()) {
                    PartETag partETag{ chunk.partNumber, outcome.GetResult().GetETag() };
                    {
                        std::lock_guard<std::mutex> lock(completedMtx);
                        completedParts.push_back(std::move(partETag));
                    }
                    uploadedBytes += chunk.size;
                } else {
                    std::cerr << "UploadPart " << chunk.partNumber << " failed: " << outcome.GetError().GetMessage() << std::endl;
                }
                free(chunk.data);
            }
        });
    }

    int fd = open(filePath.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        std::cerr << "Failed to open file with O_DIRECT: " << filePath << std::endl;
        return false;
    }

    off_t fileSize = lseek(fd, 0, SEEK_END);
    size_t partNumber = 1;

    for (off_t offset = 0; offset < fileSize; offset += PART_SIZE, ++partNumber) {
        size_t partSize = std::min<size_t>(PART_SIZE, fileSize - offset);

        void* raw = nullptr;
        if (posix_memalign(&raw, ALIGNMENT, partSize) != 0) {
            std::cerr << "Aligned memory allocation failed" << std::endl;
            break;
        }
        memset(raw, 0, partSize);

        size_t bytesReadTotal = 0;
        while (bytesReadTotal < partSize) {
            size_t toRead = std::min<size_t>(CHUNK_SIZE, partSize - bytesReadTotal);
            ssize_t bytesRead = pread(fd, (char*)raw + bytesReadTotal, toRead, offset + bytesReadTotal);
            if (bytesRead <= 0) {
                std::cerr << "Direct I/O read failed at offset " << offset + bytesReadTotal << std::endl;
                free(raw);
                close(fd);
                return false;
            }
            bytesReadTotal += bytesRead;
        }

        queue.push({ static_cast<int>(partNumber), bytesReadTotal, static_cast<char*>(raw) });
    }

    close(fd);
    queue.set_done();
    for (auto& t : workers) t.join();

    std::cout << "Upload complete: " << uploadedBytes / (1024 * 1024) << " MB uploaded." << std::endl;

    return CompleteMultipartUpload(client, bucket, key, uploadId, completedParts);
}

// Download single file (simple GetObject)
bool DownloadSingle(std::shared_ptr<S3CrtClient> client, const std::string& bucket, const std::string& key, const std::string& filePath) {
    GetObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "GetObject failed: " << outcome.GetError().GetMessage() << std::endl;
        return false;
    }

    auto& stream = outcome.GetResultWithOwnership().GetBody();
    std::ofstream output(filePath.c_str(), std::ios::binary);
    if (!output) {
        std::cerr << "Failed to open file for writing: " << filePath << std::endl;
        return false;
    }
    output << stream.rdbuf();
    return true;
}

// Download part (ranged GetObject)
void DownloadPart(std::shared_ptr<S3CrtClient> client,
                  const std::string& bucket,
                  const std::string& key,
                  uint64_t start,
                  uint64_t end,
                  std::vector<char>& buffer,
                  size_t index,
                  std::mutex& mtx,
                  std::vector<bool>& completedFlags)
{
    GetObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    char rangeHeader[64];
    snprintf(rangeHeader, sizeof(rangeHeader), "bytes=%llu-%llu", (unsigned long long)start, (unsigned long long)end);
    request.SetRange(rangeHeader);

    auto outcome = client->GetObject(request);
    if (!outcome.IsSuccess()) {
        std::cerr << "DownloadPart failed: " << outcome.GetError().GetMessage() << std::endl;
        return;
    }

    auto& stream = outcome.GetResultWithOwnership().GetBody();
    std::vector<char> tempBuffer((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());

    // Copy downloaded data into correct buffer position
    {
        std::lock_guard<std::mutex> lock(mtx);
        std::copy(tempBuffer.begin(), tempBuffer.end(), buffer.begin() + (index * PART_SIZE));
        completedFlags[index] = true;
    }
}

// Multipart download main function
bool MultipartDownload(std::shared_ptr<S3CrtClient> client,
                       const std::string& bucket,
                       const std::string& key,
                       const std::string& filePath,
                       uint64_t fileSize,
                       size_t threadCount)
{
    std::ofstream output(filePath.c_str(), std::ios::binary | std::ios::trunc);
    if (!output) {
        std::cerr << "Failed to open file for writing: " << filePath << std::endl;
        return false;
    }
    output.close();

    size_t partCount = (fileSize + PART_SIZE - 1) / PART_SIZE;
    std::vector<char> buffer(fileSize);
    std::vector<bool> completedFlags(partCount, false);
    std::mutex mtx;
    std::vector<std::thread> threads;

    for (size_t i = 0; i < partCount; ++i) {
        uint64_t start = i * PART_SIZE;
        uint64_t end = std::min(start + PART_SIZE - 1, fileSize - 1);

        // Limit number of concurrent threads
        while (threads.size() >= threadCount) {
            for (auto it = threads.begin(); it != threads.end();) {
                if (it->joinable()) {
                    it->join();
                    it = threads.erase(it);
                } else {
                    ++it;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        threads.emplace_back(DownloadPart, client, bucket, key, start, end, std::ref(buffer), i, std::ref(mtx), std::ref(completedFlags));
    }

    // Join remaining threads
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    // Verify all parts downloaded
    for (bool done : completedFlags) {
        if (!done) {
            std::cerr << "Some parts failed to download." << std::endl;
            return false;
        }
    }

    // Write buffer to file
    std::ofstream outFile(filePath.c_str(), std::ios::binary | std::ios::trunc);
    if (!outFile) {
        std::cerr << "Failed to open file for final writing: " << filePath << std::endl;
        return false;
    }
    outFile.write(buffer.data(), buffer.size());
    return true;
}


// Function to get object size from S3
uint64_t GetObjectSize(std::shared_ptr<Aws::S3Crt::S3CrtClient> client,
                       const std::string& bucket,
                       const std::string& key) {
    Aws::S3Crt::Model::HeadObjectRequest headRequest;
    headRequest.SetBucket(bucket);
    headRequest.SetKey(key);

    auto headOutcome = client->HeadObject(headRequest);
    if (!headOutcome.IsSuccess()) {
        std::cerr << "Failed to get object metadata: " << headOutcome.GetError().GetMessage() << std::endl;
        return 0;
    }

    return static_cast<uint64_t>(headOutcome.GetResult().GetContentLength());
}


int main(int argc, char** argv) {
    if (argc < 6) {
        std::cerr << "Usage:\n"
                  << argv[0] << " upload|download <bucket> <key> <file_path> <thread_count> \n";
        return 1;
    }

    std::string mode(argv[1]);
    std::string bucket(argv[2]);
    std::string key(argv[3]);
    std::string filePath(argv[4]);
    size_t threadCount = static_cast<size_t>(std::stoul(argv[5]));
    bool success = false;

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    {
        Aws::S3Crt::S3CrtClientConfiguration config;
        config.region = "us-east-1";
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true; // use with caution
        config.useVirtualAddressing = false; // for non-AWS S3
        config.caFile = "/usr/local/share/ca-certificates/minio.crt";
        config.enableTcpKeepAlive = true;
        config.tcpKeepAliveIntervalMs = 30000;
        config.endpointOverride = "https://10.10.10.155:9000";

        Aws::Auth::AWSCredentials creds("minioadmin", "minioadmin");

        std::shared_ptr<S3CrtClient> client = Aws::MakeShared<S3CrtClient>("S3CRTClient", creds, config);

        if (mode == "upload") {
            uint64_t fileSize = GetFileSize(filePath);
            if (fileSize == 0) {
                std::cerr << "File does not exist or is empty: " << filePath << std::endl;
            } else if (fileSize > MULTIPART_THRESHOLD) {
                std::cout << "Performing multipart upload\n";
                success = DoMultipartUpload(client, bucket, key, filePath, threadCount);
            } else {
                std::cout << "Performing single PUT upload\n";
                success = UploadSingle(client, bucket, key, filePath);
            }
        } else if (mode == "download") {
            uint64_t objectSize = GetObjectSize(client, bucket, key);
            if (objectSize == 0) {
                std::cerr << "Failed to retrieve object size or object is empty." << std::endl;
                success = false;
            } else if (objectSize <= MULTIPART_THRESHOLD) {
                std::cout << "Performing single GET download\n";
                success = DownloadSingle(client, bucket, key, filePath);
            } else {
                std::cout << "Performing multipart download\n";
                success = MultipartDownload(client, bucket, key, filePath, objectSize, threadCount);
            }
        } else {
            std::cerr << "Unknown mode: " << mode << std::endl;
        }
    }

    Aws::ShutdownAPI(options);
    return success ? 0 : 1;
}


#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cstdlib>

const std::size_t CHUNK_SIZE = 1e6;  // e.g., a million elements at a time
const std::size_t DB_SIZE = 200 * 250 * CHUNK_SIZE;  // 200GB
const std::string DB_PATH = "/mnt/disks/fair_db/db.bin";

void writeVectorToDisk(const std::vector<int>& data, const std::string& filepath) {
    std::ofstream outfile(filepath, std::ios::binary);
    if (!outfile.is_open()) {
        std::cerr << "Error opening file for writing: " << filepath << std::endl;
        return;
    }

    std::size_t totalSize = data.size();
    outfile.write(reinterpret_cast<const char*>(&totalSize), sizeof(totalSize));

    for (std::size_t i = 0; i < totalSize; i += CHUNK_SIZE) {
        std::size_t currentChunkSize = std::min(CHUNK_SIZE, totalSize - i);
        outfile.write(reinterpret_cast<const char*>(&data[i]), currentChunkSize * sizeof(int));
    }

    outfile.close();
}

std::vector<int> readVectorFromDisk(const std::string& filepath) {
    std::ifstream infile(filepath, std::ios::binary);
    if (!infile.is_open()) {
        std::cerr << "Error opening file for reading: " << filepath << std::endl;
        return {};
    }

    std::size_t totalSize;
    infile.read(reinterpret_cast<char*>(&totalSize), sizeof(totalSize));

    std::vector<int> data(totalSize);

    for (std::size_t i = 0; i < totalSize; i += CHUNK_SIZE) {
        std::size_t currentChunkSize = std::min(CHUNK_SIZE, totalSize - i * CHUNK_SIZE);
        infile.read(reinterpret_cast<char*>(&data[i]), currentChunkSize * sizeof(int));
    }

    infile.close();
    return data;
}

void generateAndWriteHugeVector(const std::string& filepath) {
    std::ofstream outfile(filepath, std::ios::binary);
    if (!outfile.is_open()) {
        std::cerr << "Error opening file for writing: " << filepath << std::endl;
        return;
    }

    std::size_t totalSize = DB_SIZE;
    outfile.write(reinterpret_cast<const char*>(&totalSize), sizeof(totalSize));

    std::vector<int> chunk(CHUNK_SIZE);

    for (std::size_t i = 0; i < totalSize; i += CHUNK_SIZE) {
        std::generate(chunk.begin(), chunk.end(), rand);
        outfile.write(reinterpret_cast<const char*>(chunk.data()), CHUNK_SIZE * sizeof(int));
    }

    outfile.close();
}

int main() {
    const std::string filepath = DB_PATH;

    // Test: Generate, write, read, and display some data
    generateAndWriteHugeVector(filepath);
    // std::vector<int> readData = readVectorFromDisk(filepath);
    // for (int val : readData) {
    //     std::cout << val << " ";
    // }
    // std::cout << std::endl;

    return 0;
}

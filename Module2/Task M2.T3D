#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>
#include <sstream>
#include <map>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>

using namespace std;

// Traffic Data Structure
struct TrafficData {
    string timestamp;
    string light_id;
    int cars;
};

// Shared Queue for Producer-Consumer
queue<TrafficData> trafficQueue;
mutex mtx;
condition_variable cv;
bool finished = false;

// Function to Generate Traffic Data
void generateTrafficData(int numSignals, int hours, const string& filename) {
    ofstream file(filename);
    if (!file) {
        cerr << "Error opening file!" << endl;
        return;
    }

    vector<string> signals;
    for (int i = 1; i <= numSignals; ++i) {
        signals.push_back("L" + to_string(i)); 
    }

    srand(time(0)); 

    for (int h = 0; h < hours; ++h) {
        for (int m = 0; m < 12; ++m) { 
            int minutes = m * 5;
            string timestamp = string("08:") + (minutes < 10 ? "0" : "") + to_string(minutes);

            for (const auto& signal : signals) {
                int cars = rand() % 100 + 5; 
                file << timestamp << "," << signal << "," << cars << "\n";
            }
        }
    }

    file.close();
    cout << "Traffic data generated in " << filename << endl;
}

// Producer Function to reads traffic data 
void producer(const string& filename) {
    ifstream file(filename);
    string line;

    while (getline(file, line)) {
        stringstream ss(line);
        string timestamp, light_id, cars_str;
        getline(ss, timestamp, ',');
        getline(ss, light_id, ',');
        getline(ss, cars_str, ',');

        TrafficData data = {timestamp, light_id, stoi(cars_str)};

        unique_lock<mutex> lock(mtx);
        trafficQueue.push(data);
        cv.notify_one(); 
    }

    unique_lock<mutex> lock(mtx);
    finished = true;
    cv.notify_all();
}

// Consumer Function to processes traffic data and find top congested lights
void consumer(int N) {
    map<string, int> congestion_map;

    while (true) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [] { return !trafficQueue.empty() || finished; });

        while (!trafficQueue.empty()) {
            TrafficData entry = trafficQueue.front();
            trafficQueue.pop();
            congestion_map[entry.light_id] += entry.cars;
        }

        if (finished && trafficQueue.empty()) break; 
    }

    vector<pair<string, int>> sorted_congestion(congestion_map.begin(), congestion_map.end());
    sort(sorted_congestion.begin(), sorted_congestion.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    cout << "Top " << N << " most congested traffic lights:\n";
    for (int i = 0; i < min(N, (int)sorted_congestion.size()); ++i) {
        cout << sorted_congestion[i].first << " - " << sorted_congestion[i].second << " cars\n";
    }
}

int main() {
    int numSignals = 10;  
    int hours = 1;        
    string filename = "traffic_data.txt";
    int N = 3;           

    // Generate Traffic Data
    generateTrafficData(numSignals, hours, filename);

    // Start Producer and Consumer Threads
    thread prodThread(producer, filename);
    thread consThread(consumer, N);

    prodThread.join();
    consThread.join();

    return 0;
}

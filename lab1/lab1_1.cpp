#include <iostream>
#include <stdio.h>
#include <thread>
#include <omp.h>
#include <vector>
#include <random>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <algorithm>
using namespace std;

void test(const int len) {
    const int len1 = pow(10, len);
    cout << "f " << len1 << endl;
    vector<int> v(len1);
    random_device r;
    generate(v.begin(), v.end(), [&] {return r(); });

    for (int j = 0; j < 10; j++) {
        int max = v[0];
        auto start = chrono::high_resolution_clock::now();
        #pragma omp parallel shared(v) num_threads(j)
        for (int i = 0; i < len1; i++) {
            if (v[i] > max) {
                max = v[i];
            }
            //cout << "Thread(" << omp_get_thread_num() << ")" << "v_out = " << v_out[i] << endl;
        }
        auto stop = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(stop - start);
        cout << "num_threads = " << j + 1 << ", time = " << duration.count() << endl;
    }
}

int main(int argc, char* argv[])
{
    const int len = atoi(argv[1]);
    test(len);
    return 1337;
}
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

void test(const int size) {
    cout << "matrix size = " << size << endl;

    srand(time(NULL)); // Инициализируем генератор случайных чисел
    int** a = new int* [size]; // Создаем массив указателей

    for (int i = 0; i < size; i++) {
        a[i] = new int[size]; // Создаем элементы
    }
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            a[i][j] = rand() % 10; // Каждый элемент случайному числу от 0 до 9
            //cout << a[i][j] << " "; // Вывести элементы на консольку
        }
        //cout << endl; // Двумерный массив. Строка кончилась, переводим строку и на консоли
    }
 
    int** b = new int* [size];
    for (int i = 0; i < size; i++) {
        b[i] = new int[size];
    }
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            b[i][j] = rand() % 10;
            //cout << b[i][j] << " ";
        }
        //cout << endl;
    }

    int** c = new int* [size];

    for (int i = 0; i < size; i++) {
        c[i] = new int[size];
    }

    /*cout << "matrix a:" << endl;

    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            cout << a[i][j] << " ";
        }
        cout << endl;   
    }*/

    /*cout << "matrix b:" << endl;

    for (int i = 0; i < size; i++)
    {
        for (int j = 0; j < size; j++)
        {
            cout << b[i][j] << " ";
        }
        cout << endl;
    }*/

    auto start1 = chrono::high_resolution_clock::now();

        #pragma omp parallel num_threads(1)
    {
        #pragma omp for
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                c[i][j] = 0;
                for (int k = 0; k < size; k++) {
                    c[i][j] += a[i][k] * b[k][j];
                }
            }
        }
    }
    auto stop1 = chrono::high_resolution_clock::now();
    double t1;
    auto dur1 = chrono::duration_cast<chrono::milliseconds>(stop1 - start1);
    t1 = dur1.count();
    //cout << "t1 = " << t1 << endl;
    for (int nn = 1; nn < 11; nn++) {

        auto start = chrono::high_resolution_clock::now();

        #pragma omp parallel num_threads(nn+1)
        {
            #pragma omp for
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    c[i][j] = 0;
                    for (int k = 0; k < size; k++) {
                        c[i][j] += a[i][k] * b[k][j];
                    }
                }
            }
        }
        /*cout << "matrix c:" << endl;
        for (int i = 0; i < size; i++)
        {
            for (int j = 0; j < size; j++)
            {
                cout << c[i][j] << " ";
            }
            cout << endl;
        }*/

        auto stop = chrono::high_resolution_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(stop - start);
        cout << "num threads = " << nn << ", t1/t = " << t1 / duration.count() << endl;
    }
}

int main(int argc, char* argv[])
{
    int n = atoi(argv[1]);
    const int size = n * 1000;
    test(size);
    return 1337;
}
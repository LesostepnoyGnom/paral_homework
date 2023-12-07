#include <iostream>
using namespace std;

int main(int argc, char* argv[])
{
    setlocale(LC_ALL, "RU");

    int c = 1;
    bool ads = false;
    cout << "your string: " << argv[1] << endl;

    for (char* i = argv[1]; *i; ++i) {

        if (*i == ' ') {
            if (!ads) 
                c++;
            ads = true;
        }
        else ads = false;
    }
    cout << "words: " << c;







    //cout << "Hello World!\n";

    //short a;
    //cout << "Введите число: ";
    //cin >> a;
    //cout << "Ваше число: " << a << endl;

    //if (a > 0) cout << "Ваше число больше 0\n"; // если одно действие, то можно без {}
    //else if (a == 0) cout << "Число равно 0\n";
    //else cout << "Число меньше 0\n";

    //if (a == 300 || a == -300) {    // AND записывается как &&
    //    cout << "\n";
    //    cout << "Отсоси у тракториста\n";
    //    cout << "за мат соре\n";
    //}

    //short l = 5;
    //cout << "Сделаем массив из " << 4 << " элементов\n";
    //int* nums = new int[4]; // массив из трёх элементов
    //for (short i = 0; i < 4; i++) {
    //    cout << "Введите " << i + 1 << " число массива" << endl;
    //    cin >> nums[i];
    //}

    //cout << "Ваш массив: " << nums << endl;
    //cout << "1 элемент массива = " << nums[0] << endl;
    //delete[] nums;

    //short nums2[3] = { 1,2,3 };

    //string strg = "gihwhwfnwgmqikwdwkjfi";
    //cout << strg;
}
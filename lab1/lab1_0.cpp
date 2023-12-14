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
}

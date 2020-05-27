#include <iostream>
#include <fstream>

#include <sqlite3.h>

using namespace std;

int main(void) {
    sqlite3 *db;
    int res;
    res = sqlite3_open("./test.sqlite", &db);

    if (res) {
        cerr << "Can not open database: " << sqlite3_errmsg(db) << endl;
        return (1);
    }

    

    sqlite3_close(db);
}
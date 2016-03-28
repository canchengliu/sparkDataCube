#include <iostream>
#include <fstream>
#include <ctime>
#include <cmath>
#include <cstring>
#include <cstdlib>

using namespace std;

/************************************************************
description: generates a random number between _min and _max 
             uniformly     

para:   - min: left margin of uniform distribution
        - max: right margin of uniform distribution

return: - integer value distributed from [min, max]

by: qiang huang
last touch: 6 Jan 2014
************************************************************/

int uniform(int _min, int _max) {
    double base = RAND_MAX - 1;
    double f_r  = ((double) rand()) / base;
    
    int ret = floor(((double)_max - (double)_min + 1.0) * f_r + (double)_min);
    return ret;
}

/************************************************************
description: generates a random number between _min and _max 
             uniformly     

para:   - min: left margin of uniform distribution
        - max: right margin of uniform distribution

return: - real value distributed from [min, max]

by: qiang huang
last touch: 6 Jan 2014
************************************************************/

double uniform2(double _min, double _max) {
    int int_r = rand();
    double base = RAND_MAX - 1;
    double f_r  = ((double) int_r) / base;
    
    return (_max - _min) * f_r + _min;
}

int categoryGenerate()
{
    //int dis[10] = {75,10,5,1,1,1,1,2,2,2};
    int dis[10] = {30,30,30,1,1,1,1,2,2,2}; 
    int split[10];

    int count = 0;
    int first;

    for (int i = 0; i < 10; i++)
    {
        count += dis[i];
        split[i] = count;
    }

    int pro = rand() % 100;
            
    for (int k = 0; k < 10; k++)
    {
        if (pro < split[k])
        {
            first = k;
            break;
        }
    }

    return first + 1;
}

int main(int argc, char *argv[]) 
{
    char filename[100] = "productTable_";
    int fnlen = strlen(filename);

    srand(time(NULL));
    
    int productid_min = 1;
    int productid_max = 1000000;
    
    int country_min = 1;
    int country_max = 3;
    
    int category_min = 1;
    int category_max = 10;
    
    double sales_min = 0.0;
    double sales_max = 10000.0;
    
    long i;

    long N_tuples = atoi(argv[1]);
    char num[20];
    sprintf(num, "%ld", N_tuples);
    
    strcat(filename, num);
    strcat(filename, ".txt");
    
    ofstream outfile;
    outfile.open(filename);
    
    int productid = 0;
    int category = 0;
    int subcategory = 0;
    int product = 0; 
    double sales = 0.0;
    
    for (i = 0; i < N_tuples; i++) {
        productid = uniform(productid_min, productid_max);
        
        //category = uniform(category_min, category_max);
        category = categoryGenerate();
        subcategory = uniform((category - 1) * 20 + 1, (category - 1) * 20 + 20);
        product = uniform((category - 1) * 2000 + 1, (category - 1) * 2000 + 2000);
        
        sales = uniform(sales_min, sales_max);
/*      
        outfile << i + 1 << " " << productid << " " << country << " " << state << " " << city << " " 
                << category << " " << subcategory << " " << product << " " << sales << endl;
*/
        outfile << i + 1 << "\t" << productid << "\t" << category << "\t" << subcategory << "\t" << product / 20.0
                << "\t" << sales << endl;
    }
    outfile.close();
    
    //system("pause");
    return 0;
}

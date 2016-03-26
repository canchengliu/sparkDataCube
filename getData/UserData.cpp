#include <iostream>
#include <fstream>
#include <ctime>
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <string>
using namespace std;

/************************************************************
description: generates a random number between _min and _max 
			 uniformly     

para:	- min: left margin of uniform distribution
		- max: right margin of uniform distribution

return:	- integer value distributed from [min, max]

by:	qiang huang
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

para:	- min: left margin of uniform distribution
		- max: right margin of uniform distribution

return:	- real value distributed from [min, max]

by:	qiang huang
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
    char filename[100] = "userTable_";
	int fnlen = strlen(filename);

    srand(time(NULL));
    
    int userid_min = 1;
    int userid_max = 1000000;
    
    int country_min = 1;
    int country_max = 3;
    
    int category_min = 1;
    int category_max = 10;
    
    double buy_min = 0.0;
    double buy_max = 10000.0;
    
    long i;

    long N_tuples = atoi(argv[1]);
    char num[20];
    sprintf(num, "%ld", N_tuples);
    
    strcat(filename, num);
    strcat(filename, ".txt");
    
    ofstream outfile;
    outfile.open(filename);
    
    int userid = 0;
    int sex = 0; // 0 for male; 1 for female
    string name = "";
    int country = 0;
    int province = 0;
    int city = 0;
    int category = 0;
    int subcategory = 0;
    int visit = 0; 
    double buy = 0.0;
    
    for (i = 0; i < N_tuples; i++) {
        userid = uniform(userid_min, userid_max);
        sex = (rand() % 10 > 5 ? 0 : 1); // set female > male
        name.push_back(char(rand() % 26 + 'a'));
        
        country = uniform(country_min, country_max);
        if (country == 1) {
            province = uniform(1, 50);
            city = uniform(1, 2000);
        }
        else if (country == 2) {
            province = uniform(51, 60);
            city = uniform(2001, 2500);
        }
        else {
            province = uniform(61, 100);
            city = uniform(2501, 4000);
        }
        
        //category = uniform(category_min, category_max);
		category = categoryGenerate();
        subcategory = uniform((category - 1) * 20 + 1, (category - 1) * 20 + 20);
        visit = uniform((category - 1) * 2000 + 1, (category - 1) * 2000 + 2000);
        
        buy = uniform2(buy_min, buy_max);
/*      
        outfile << i + 1 << " " << userid << " " << country << " " << province << " " << city << " " 
                << category << " " << subcategory << " " << visit << " " << buy << endl;
*/
        outfile << i + 1 << "\t" << userid << "\t" << sex << "\t" << name << "\t" << country << "\t" << province << "\t" << city << "\t" 
                << category << "\t" << subcategory << "\t" << visit / 100 << "\t" << buy << endl;
        name = "";
    }
    outfile.close();
    
    //system("pause");
    return 0;
}

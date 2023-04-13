
#include <stdio.h> 
#include <sys/stat.h> 
#include <fcntl.h> 
#include <stdarg.h>
#include "int_test_result.h"

#define BASE_DIR  "output"


void mark_result(char *casename, char result)
{
    char output_filename[256];
    int  fd;
    
    if( result == RESULT_PASS )
    {
        sprintf(output_filename, "%s_PASS.txt", casename);
        printf("\n\n%s PASS\n\n", casename);
    }
    else
    {
        sprintf(output_filename, "%s_FAIL.txt", casename);
        printf("\n\n%s FAIL\n\n", casename);
    }
    

    if ((fd = open(output_filename, 
                    O_WRONLY | O_CREAT | O_TRUNC,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
    {
        perror("Cannot open output file\n");
    }

    close(fd);
}



void tlog(char *prefix, char * fmt, ...)
{
    if( prefix && fmt )
    {
        va_list ap;
        va_start(ap, fmt);

        
        printf("%s (%d): ", prefix, time(NULL));
        vprintf(fmt, ap);
        printf("\n");

        va_end(ap);
    }
}


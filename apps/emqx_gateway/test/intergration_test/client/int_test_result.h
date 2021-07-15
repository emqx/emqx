
#ifndef _INT_TEST_RESULT_H_
#define _INT_TEST_RESULT_H_


#define RESULT_PASS  1
#define RESULT_FAIL  0


void mark_result(char *caesname, char result);
void tlog(char *prefix, char * fmt, ...);

#endif  // _INT_TEST_RESULT_H_

#include "bing_log.h"

class TT{
public:
	bingLog BLog;
	TT(){};
	~TT(){};
};

int main(){
	TT t1;
	t1.BLog.log("%s %d\n", __FILE__, __LINE__);

	for(int i = 0; i < 1000; ++i){
		t1.BLog.log("This is log %05d, in %s %05d\n", i, __FILE__, __LINE__);
	}

	return 0;
}

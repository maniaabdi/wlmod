#include <fstream>
#include <string>
#include <regex>
#include "caches/lru_variants.h"
#include "caches/gd_variants.h"
#include "request.h"

using namespace std;

int main (int argc, char* argv[])
{
  size_t hit_size = 0;
  size_t total_size = 0;
  // output help if insufficient params
  if(argc < 4) {
    cerr << "webcachesim traceFile cacheType cacheSizeBytes [cacheParams]" << endl;
    return 1;
  }

  // trace properties
  const char* path = argv[1];

  // create cache
  const string cacheType = argv[2];
  unique_ptr<Cache> webcache = Cache::create_unique(cacheType);
  if(webcache == nullptr)
    return 1;

  // configure cache size
  const uint64_t cache_size  = std::stoull(argv[3]);
  webcache->setSize(cache_size);

  // parse cache parameters
  regex opexp ("(.*)=(.*)");
  cmatch opmatch;
  string paramSummary;
  for(int i=4; i<argc; i++) {
    regex_match (argv[i],opmatch,opexp);
    if(opmatch.size()!=3) {
      cerr << "each cacheParam needs to be in form name=value" << endl;
      return 1;
    }
    webcache->setPar(opmatch[1], opmatch[2]);
    paramSummary += opmatch[2];
  }

  ifstream infile;
  long long reqs = 0, hits = 0;
  long long t, id, size;

  cerr << "running..." << endl;

  infile.open(path);
  SimpleRequest* req = new SimpleRequest(0, 0);
  while (infile >> t >> id >> size)
    {
        reqs++;
        
        req->reinit(id,size);
	total_size += req->getSize();
        if(webcache->lookup(req)) {
	    hit_size += req->getSize(); 
            hits++;
        } else {
            webcache->admit(req);
        }
    }

  delete req;

  infile.close();
  cout << cacheType << " , cache size: " << cache_size << " , cache parameter: " << paramSummary << "\n" 
       << "Object count: " << reqs << " , Object hit count " << hits << ", object hit ratio: " << double(hits)/reqs << "\n"
        << "Byte count: " << total_size << " , Byte hit count " << hit_size << ", Byte hit ratio: " << double(hit_size)/total_size << "\n"
       << endl;

  return 0;
}

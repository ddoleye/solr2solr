Solr2Solr
===============

Solr 데이터를 다른 Solr로 복사합니다. 

원본 Solr 에서 json 형식으로 데이터를 가져와서(wt=json) 다른 Solr 에 POST(/update/json)를 합니다.
[Deep Paing](http://yonik.com/solr/paging-and-deep-paging/) 방식으로 데이터를 가져오며 원본 Solr 에서 제공하는 필드만 복사됩니다.

```sh
$ java -jar target/solr2solr-jar-with-dependencies.jar
usage: java -jar app.jar [OPTIONS] source-solr-url target-solr-url
 -c,--cursor <arg>      cursorMark. 기존 작업을 이어서 실행할 때
 -commitWithin <arg>    commitWithin. 기본값은 10000
 -q,--query <arg>       q 파라미터. 기본값은 *:*
 -r,--rows <arg>        페이지당 요청 데이터 건수
 -u,--uniqueKey <arg>   <uniqueKey> 필드. 기본값은 'id'
$ java -jar target/solr2solr-jar-with-dependencies.jar http://localhost:8983/solr/collection http://solrcloud:8983/solr/collection 
```


create keyspace if not exists test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

use test;

create type information (
  info1 text,
  info2 text
);


create table product (
  id text primary key,
  details frozen<map<text, frozen<information>>>,
  moreDetails frozen<list<information>>
);

insert into product(id, details, moreDetails) values('test', {'k1':{info1:'v-info1',info2:'v-info2'}},[{info1:'l-info1',info2:'linfo2'}]);

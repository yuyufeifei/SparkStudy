package com.gzh.spark.streaming.example

object Sql {
  /* 一
  CREATE TABLE black_list (userid CHAR(1) PRIMARY KEY);
  CREATE TABLE user_ad_count (
    dt varchar(255),
    userid CHAR (1),
    adid CHAR (1),
    count BIGINT,
    PRIMARY KEY (dt, userid, adid)
  );
  */

  /* 二
  CREATE TABLE area_city_ad_count (
    dt VARCHAR(255),
    area VARCHAR(255),
    city VARCHAR(255),
    adid VARCHAR(255),
     count BIGINT,
    PRIMARY KEY (dt,area,city,adid)
    );
   */

}

CREATE SCHEMA IF NOT EXISTS "alzheimer";

CREATE  TABLE "alzheimer".dim_class ( 
	class_id             varchar(255)  NOT NULL  ,
	class_desc           varchar(255)    ,
	CONSTRAINT pk_dim_class PRIMARY KEY ( class_id )
 );

CREATE  TABLE "alzheimer".dim_location ( 
	location_id          integer  NOT NULL  ,
	location_desc        varchar(255)    ,
	latitude             numeric(3,10)    ,
	longitude            numeric(3,10)    ,
	CONSTRAINT pk_dim_location PRIMARY KEY ( location_id )
 );

CREATE  TABLE "alzheimer".dim_question ( 
	question_id          varchar(255)  NOT NULL  ,
	question_desc        varchar(255)    ,
	CONSTRAINT pk_dim_question PRIMARY KEY ( question_id )
 );

CREATE  TABLE "alzheimer".dim_stratification ( 
	stratification_id    integer  NOT NULL  ,
	stratification1      varchar(255)    ,
	stratification2      varchar(255)    ,
	stratification_category2 varchar(255)    ,
	CONSTRAINT pk_dim_stratification PRIMARY KEY ( stratification_id )
 );

CREATE  TABLE "alzheimer".dim_topic ( 
	topic_id             varchar(255)  NOT NULL  ,
	topic_desc           varchar(255)    ,
	CONSTRAINT pk_dim_topic PRIMARY KEY ( topic_id )
 );

CREATE  TABLE "alzheimer".dim_year ( 
	year_id              integer  NOT NULL  ,
	"year"               integer    ,
	CONSTRAINT pk_dim_year PRIMARY KEY ( year_id )
 );

CREATE  TABLE "alzheimer".facts_data_value_mean ( 
	fact_id              integer  NOT NULL  ,
	data_value           numeric(2,1)    ,
	low_confidence_limit numeric(2,1)    ,
	high_confidence_limit numeric(2,1)    ,
	location_id          integer  NOT NULL  ,
	topic_id             varchar(255)  NOT NULL  ,
	class_id             varchar(255)  NOT NULL  ,
	year_start           integer  NOT NULL  ,
	year_end             integer  NOT NULL  ,
	question_id          varchar  NOT NULL  ,
	stratification_id    integer  NOT NULL  ,
	CONSTRAINT pk_facts_data_value_mean PRIMARY KEY ( fact_id, class_id, location_id, question_id, year_start, year_end, topic_id, stratification_id )
 );

CREATE  TABLE "alzheimer".facts_data_value_percentage ( 
	fact_id              integer  NOT NULL  ,
	data_value           numeric(2,1)    ,
	low_confidence_limit numeric(2,1)    ,
	high_confidence_limit numeric(2,1)    ,
	location_id          integer  NOT NULL  ,
	topic_id             varchar(255)  NOT NULL  ,
	class_id             varchar  NOT NULL  ,
	year_start           integer  NOT NULL  ,
	year_end             integer  NOT NULL  ,
	question_id          varchar(255)  NOT NULL  ,
	stratification_id    varchar(255)  NOT NULL  ,
	CONSTRAINT pk_facts_data_value_percentage PRIMARY KEY ( fact_id, class_id, location_id, question_id, year_start, year_end, topic_id, stratification_id )
 );

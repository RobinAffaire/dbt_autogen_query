use role accountadmin;

create database if not exists flat_file;
use database flat_file;
create schema if not exists seed;


create schema if not exists flat_file.base;
create schema if not exists flat_file.staging;

create database if not exists tech;
create schema if not exists tech.intermediate;

create database if not exists sap_p93;
create schema if not exists sap_p93.raw;
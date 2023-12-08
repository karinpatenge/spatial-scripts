-----------
-- Clean up
-----------
delete
from user_sdo_geom_metadata
where table_name like 'SPLIT_POLYGON_TEST%';

commit;

drop table split_polygon_test_input purge;
drop table split_polygon_test_result purge;

-----------------------------
-- Create DB objects for test
-----------------------------

create table split_polygon_test_input (
    id number,
    testcase number(4),
    name varchar2(50),
    geom sdo_geometry);

create table split_polygon_test_result (
    id number,
    testcase number(4),
    name varchar2(50),
    geom sdo_geometry);

insert into user_sdo_geom_metadata (
    table_name,
    column_name,
    diminfo,
    srid)
values(
    'split_polygon_test_input',
    'geom',
    mdsys.sdo_dim_array (
        mdsys.sdo_dim_element ('longitude', -180, 180, 10),
        mdsys.sdo_dim_element ('latitude', -90, 90, 10)),
        4326);

insert into user_sdo_geom_metadata (
    table_name,
    column_name,
    diminfo,
    srid)
values(
    'split_polygon_test_result',
    'geom',
    mdsys.sdo_dim_array (
        mdsys.sdo_dim_element ('longitude', -180, 180, 10),
        mdsys.sdo_dim_element ('latitude', -90, 90, 10)),
        4326);

commit;

create index split_polygon_test_input_geom_sidx on split_polygon_test_input (geom)
    indextype is mdsys.spatial_index_v2;
create index split_polygon_test_result_geom_sidx on split_polygon_test_result (geom)
    indextype is mdsys.spatial_index_v2;

-------------------
-- Insert test data
-------------------

truncate table split_polygon_test_input;
truncate table split_polygon_test_result;

-- Polygons
insert into split_polygon_test_input (
    select
        101,
        1,
        'Schleswig-Holstein',
        geom
    from deu_adm1
    where name_1 = 'Schleswig-Holstein');
insert into split_polygon_test_input (
    select
        201,
        2,
        'Berlin',
        geom
    from deu_adm1
    where name_1 = 'Berlin');
insert into split_polygon_test_input (
    select
        301,
        3,
        'Brandenburg',
        geom
    from deu_adm1
    where name_1 = 'Brandenburg');

-- Lines
insert into split_polygon_test_input (
    select
        111,
        1,
        'Line cutting Schleswig-Holstein',
        sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1), sdo_ordinate_array(7.618,55.1851,11.11,53.225)) as geom
    from dual);
insert into split_polygon_test_input (
    select
        211,
        2,
        'Line cutting Berlin',
        sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1), sdo_ordinate_array(13.00748,52.887629,13.809722,52.1594815)) as geom
    from dual);
insert into split_polygon_test_input (
    select
        311,
        3,
        'Line cutting Brandenburg',
        sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1), sdo_ordinate_array(10.912,52.9434,15.14,53.0763)) as geom
    from dual);
insert into split_polygon_test_input (
    select
        312,
        3,
        'Line cutting Brandenburg',
        sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1), sdo_ordinate_array(13.57485,53.6544,12.95288,51.02412)) as geom
    from dual);

commit;

-------------------------
-- Positive function test
-------------------------
truncate table split_polygon_test_result;


insert into split_polygon_test_result (
    select
        a.id,
        11,
        'Schleswig-Holstein splitted by line',
        a.geometry
    from table (
        select split_polygon(
            (select p.geom from split_polygon_test_input p where p.id = 101),
            (select l.geom from split_polygon_test_input l where l.id = 111))
        from dual) a);

insert into split_polygon_test_result (
    select
        a.id,
        21,
        'Berlin splitted by line',
        a.geometry
    from table (
        select split_polygon(
            (select p.geom from split_polygon_test_input p where p.id = 201),
            (select l.geom from split_polygon_test_input l where l.id = 211))
        from dual) a);

insert into split_polygon_test_result (
    select
        a.id,
        31,
        'Brandenburg splitted by line',
        a.geometry
    from table (
        select split_polygon(
            (select p.geom from split_polygon_test_input p where p.id = 301),
            (select l.geom from split_polygon_test_input l where l.id = 311))
        from dual) a);

insert into split_polygon_test_result (
    select
        a.id,
        32,
        'Brandenburg splitted by line',
        a.geometry
    from table (
        select split_polygon(
            (select p.geom from split_polygon_test_input p where p.id = 301),
            (select l.geom from split_polygon_test_input l where l.id = 312))
        from dual) a);

commit;

-------------------------------------------------------------------------
-- Negative function test:
--   Test error message for lines that either start or end inside the MBR
-------------------------------------------------------------------------

select
    a.id,
    a.geometry
from table (
    select split_polygon(
        p.geom,
        (select sdo_geometry(
            2002,
            4326,
            null,
            sdo_elem_info_array(1,2,1),
            sdo_ordinate_array(12.912,52.9434,14.14,53.0763))
        from dual))
    from split_polygon_test_input p
    where p.id = 301) a;

-- Returns:
--   ORA-20001: Line must intersect poly MBR two times
--   ORA-06512: at "SPATIALUSER.SPLIT_POLYGON", line 157
--   ORA-06512: at line 1
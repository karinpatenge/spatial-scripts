--------------------------------------------------------------------------------
--  Disclaimer:
--    You running this script/function means you will not blame the author(s) if this breaks your stuff. 
--    This script/function is provided AS IS without warranty of any kind. 
--    Author(s) disclaim all implied warranties including, without limitation, 
--    any implied warranties of merchantability or of fitness for a particular purpose. 
--    The entire risk arising out of the use or performance of the sample scripts and documentation remains with you. 
--    In no event shall author(s) be held liable for any damages whatsoever (including, without limitation, 
--    damages for loss of business profits, business interruption, loss of business information, 
--    or other pecuniary loss) arising out of the use of or inability to use the script or documentation. 
--    Neither this script/function, nor any part of it other than those parts that are explicitly #
--    copied from others, may be republished without author(s) express written permission. 
--    Author(s) retain the right to alter this disclaimer at any time.
--------------------------------------------------------------------------------
--  Author: Dan Geringer
--  Created: Dec 2023
--
--  Function SPLIT_POLYGON
--
--     INPUT:
--               poly_geom - polygon to split
--               line_geom - line to determine where to split.
--                           line must be a 2 point line
--                           line must intersect the polygon MBR two times
--               tolerance - tolerance associated with the input geometries
--
--     OUTPUT:
--               SDO_REGIONSET - Table function that pipes back two geometries
--                               that result from the split
--
--     EXAMPLES:
--               -- Pipes back both geometries that result from the split
--               SELECT a.id, a.geometry
--               FROM table (SELECT split_polygon(geom, sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1),
--                                                                sdo_ordinate_array(13.4,53,13.6,52)))
--                           FROM deu_adm1
--                           WHERE name_1 = 'Berlin') a;
--
--               -- Pipes back geometry with id=1, which is one of the geometries that result from the split
--               SELECT a.id, a.geometry
--               FROM table (SELECT split_polygon(geom, sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1),
--                                                                sdo_ordinate_array(13.4,53,13.6,52)))
--                           FROM deu_adm1
--                           WHERE name_1 = 'Berlin') a
--               WHERE a.id = 1;
-
--
--               -- Pipes back geometry with id=2, which is the other geometry that results from the split
--               SELECT a.id, a.geometry
--               FROM table (SELECT split_polygon(geom, sdo_geometry(2002,4326,null,sdo_elem_info_array(1,2,1),
--                                                                sdo_ordinate_array(13.4,53,13.6,52)))
--                           FROM deu_adm1
--                           WHERE name_1 = 'Berlin') a
--               WHERE a.id = 2;
--
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION split_polygon (
  poly_geom SDO_GEOMETRY,
  line_geom SDO_GEOMETRY,
  tolerance NUMBER := .05)
RETURN SDO_REGIONSET PIPELINED DETERMINISTIC PARALLEL_ENABLE AS
  bad_line       EXCEPTION;
  poly_geom_copy SDO_GEOMETRY := poly_geom;
  line_geom_copy SDO_GEOMETRY := line_geom;
  mbr_line       SDO_GEOMETRY;
  lrs_mbr_line   SDO_GEOMETRY;
  int_geom       SDO_GEOMETRY;
  p1             SDO_GEOMETRY := NULL;
  p2             SDO_GEOMETRY := NULL;
  p1_final       SDO_GEOMETRY := NULL;
  p2_final       SDO_GEOMETRY := NULL;
  element        SDO_GEOMETRY;
  m1             NUMBER;
  m2             NUMBER;
  temp_num       NUMBER;
  save_srid      NUMBER := poly_geom.sdo_srid;
BEGIN
  IF line_geom IS NOT NULL AND poly_geom IS NOT NULL
  THEN
    -- Only 2 point line is allowed for input
    IF line_geom.sdo_ordinates.count = 4 AND line_geom.sdo_gtype=2002
    THEN
      -- Perform cartesian split
      poly_geom_copy.sdo_srid := NULL;
      line_geom_copy.sdo_srid := NULL;

      -- MBR of input polygon, converted to linestring
      mbr_line := sdo_util.polygontoline(sdo_geom.sdo_mbr(poly_geom_copy));

      -- Intersect mbr_line with the input line
      int_geom := sdo_geom.sdo_intersection(mbr_line, line_geom_copy, .0000005);

      -- Only split if intersection of line and mbr_line results in 2 points
      IF int_geom IS NOT NULL
      THEN
        IF int_geom.sdo_gtype = 2005
        THEN
          -- Convert mbr_line to LRS
          lrs_mbr_line := sdo_lrs.convert_to_lrs_geom(mbr_line);

          -- Get measure of intersection points
          m1 := sdo_lrs.find_measure (lrs_mbr_line, sdo_util.extract(int_geom,1,1));
          m2 := sdo_lrs.find_measure (lrs_mbr_line, sdo_util.extract(int_geom,1,2));

          -- Ensure value of m1 is less than m2.  If not, swap values.
          IF m1 > m2
          THEN
            temp_num := m1;
            m1 := m2;
            m2 := temp_num;
          END IF;

          -- Use LRS to split the MBR into two polygons, p1 and p2.
          --   p1 and p2 will be used later to intersect with the polygon to split.
          p1 := sdo_lrs.convert_to_std_geom(sdo_lrs.clip_geom_segment (lrs_mbr_line, m1, m2));
          p1 := sdo_util.append(p1, sdo_geometry(2002,null,null,
                                      sdo_elem_info_array(1,2,1),
                                      sdo_ordinate_array(p1.sdo_ordinates(p1.sdo_ordinates.count-1),
                                                         p1.sdo_ordinates(p1.sdo_ordinates.count),
                                                         p1.sdo_ordinates(1),
                                                         p1.sdo_ordinates(2))));

          IF m2 != lrs_mbr_line.sdo_ordinates(lrs_mbr_line.sdo_ordinates.count)
          THEN
            p2 := sdo_lrs.convert_to_std_geom(
                    sdo_lrs.clip_geom_segment (lrs_mbr_line,
                                               m2,
                                               lrs_mbr_line.sdo_ordinates(lrs_mbr_line.sdo_ordinates.count)));
          END IF;
          IF m1 > 0
          THEN
            p2 := sdo_util.append (p2,
                                   sdo_lrs.convert_to_std_geom(sdo_lrs.clip_geom_segment (lrs_mbr_line, 0, m1)));
          END IF;

          p2 := sdo_util.append(p2, sdo_geometry(2002,null,null,
                                      sdo_elem_info_array(1,2,1),
                                      sdo_ordinate_array(p2.sdo_ordinates(p2.sdo_ordinates.count-1),
                                                         p2.sdo_ordinates(p2.sdo_ordinates.count),
                                                         p2.sdo_ordinates(1),
                                                         p2.sdo_ordinates(2))));


          -- Make p1 and p2 polygons again, and rectify with cartesian tolerance.
          p1.sdo_gtype := 2003;
          p2.sdo_gtype := 2003;
          p1.sdo_elem_info := sdo_elem_info_array(1,1003,1);
          p2.sdo_elem_info := sdo_elem_info_array(1,1003,1);
          p1 := sdo_util.rectify_geometry(p1, .0000005);
          p2 := sdo_util.rectify_geometry(p2, .0000005);
        ELSE
          -- Line did not intersect with poly MBR in two places, raise error
          RAISE bad_line;
        END IF;
      ELSE
        RAISE bad_line;
      END IF;

      -- Perform cartesian instersection
      p1 := sdo_geom.sdo_intersection (poly_geom_copy, p1, .0000005);
      p2 := sdo_geom.sdo_intersection (poly_geom_copy, p2, .0000005);

      -- Restore SRID
      p1.sdo_srid := save_srid;
      p2.sdo_srid := save_srid;

      -- Rectify p1 if it's not valid
      IF sdo_geom.validate_geometry_with_context(p1,tolerance) != 'TRUE'
      THEN
        p1 := sdo_util.rectify_geometry(p1,tolerance);
      END IF;

      -- Rectify p2 if it's not valid
      IF sdo_geom.validate_geometry_with_context(p2,tolerance) != 'TRUE'
      THEN
        p2 := sdo_util.rectify_geometry(p2,tolerance);
      END IF;

      -- Remove any elements that are not polygons to avoid gtype 2004
      IF p1 IS NOT NULL
      THEN
        FOR r IN 1 .. sdo_util.getnumelem(p1) LOOP
          element := sdo_util.extract (p1, r);
          IF element.sdo_gtype = 2003
          THEN
            p1_final := sdo_util.append(p1_final, element);
          END IF;
        END LOOP;
      END IF;

      -- Pipe back first split polygon. Assign it id=1
      PIPE ROW (sdo_region(1, p1_final));

      -- Remove any elements that are not polygons to avoid gtype 2004
      IF p2 IS NOT NULL
      THEN
        FOR r IN 1 .. sdo_util.getnumelem(p2) LOOP
          element := sdo_util.extract (p2, r);
          IF element.sdo_gtype = 2003
          THEN
            p2_final := sdo_util.append(p2_final, element);
          END IF;
        END LOOP;
      END IF;

      -- Pipe back second split polygon. Assign it id=2
      PIPE ROW (sdo_region(2, p2_final));
    END IF;
  END IF;

  EXCEPTION
    WHEN bad_line THEN
      RAISE_APPLICATION_ERROR(-20001,'Line must intersect poly MBR two times');
END;
/


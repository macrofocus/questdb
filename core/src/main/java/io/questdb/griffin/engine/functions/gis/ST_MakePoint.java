/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.gis;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTWriter;

public class ST_MakePoint implements FunctionFactory {

    private static final String SYMBOL = "st_makepoint";
    private static final String SIGNATURE = SYMBOL + "(DD)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int type = ColumnType.STRING;

        final Function lonArg = args.get(0);
        final Function latArg = args.get(1);
        final boolean isLonConst = lonArg.isConstant();
        final boolean isLatConst = latArg.isConstant();

        if (isLonConst && isLatConst) {

            double lon = lonArg.getDouble(null);
            if (lon < -180.0 || lon > 180.0) {
                throw SqlException.$(argPositions.getQuick(0), "longitude must be in [-180.0..180.0] range");
            }

            double lat = latArg.getDouble(null);
            if (lat < -90.0 || lat > 90.0) {
                throw SqlException.$(argPositions.getQuick(1), "latitude must be in [-90.0..90.0] range");
            }

            return new ST_MakePointConstant(lon, lat);
        } else {
            return new ST_MakePointFunction(lonArg, latArg);
        }
    }

    private static class ST_MakePointFunction extends StrFunction {
        private final Function lat;
        private final Function lon;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ST_MakePointFunction(Function lon, Function lat) {
            this.lon = lon;
            this.lat = lat;
        }

        @Contract(pure = true)
        @Override
        public @Nullable CharSequence getStrA(Record rec) {
            final double x = lon.getDouble(rec);
            final double y = lat.getDouble(rec);
            GeometryFactory geometryFactory = new GeometryFactory();
            Coordinate coord = new Coordinate(x, y);
            Point point = geometryFactory.createPoint(coord);

            // Convert the Geometry to WKT format
            WKTWriter wktWriter = new WKTWriter();
            String wkt = wktWriter.write(point);
            sinkA.clear();
            sinkA.put(wkt);
            return sinkA;        }

        @Override
        public CharSequence getStrB(Record rec) {
            final double value = lat.getDouble(rec);
            if (Double.isNaN(value)) {
                return null;
            }
            sinkB.clear();
            sinkB.put("PPPB" + value);
            return sinkB;
        }


        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(lon).val(',').val(lat).val(')');
        }
    }

    private static class ST_MakePointConstant extends StrFunction {
        private final Double lat;
        private final Double lon;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ST_MakePointConstant(Double lon, Double lat) {
            this.lon = lon;
            this.lat = lat;
        }

        @Contract(pure = true)
        @Override
        public @Nullable CharSequence getStrA(Record rec) {
            GeometryFactory geometryFactory = new GeometryFactory();
            Coordinate coord = new Coordinate(lon, lat);
            Point point = geometryFactory.createPoint(coord);

            // Convert the Geometry to WKT format
            WKTWriter wktWriter = new WKTWriter();
            String wkt = wktWriter.write(point);
            sinkA.clear();
            sinkA.put(wkt);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return null;
        }


        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(lon).val(',').val(lat).val(')');
        }
    }
}

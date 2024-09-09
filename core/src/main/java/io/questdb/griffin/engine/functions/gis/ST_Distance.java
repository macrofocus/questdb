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
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class ST_Distance implements FunctionFactory {

    private static final String SYMBOL = "st_distance";
    private static final String SIGNATURE = SYMBOL + "(ØØ)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        int type = ColumnType.STRING;

        final Function geomA = args.get(0);
        final Function geomB = args.get(1);
        final boolean isGeomAConst = geomA.isConstant();
        final boolean isGeomBConst = geomB.isConstant();

        if (isGeomAConst && isGeomBConst) {
            CharSequence ga = geomA.getStrA(null);
            CharSequence gb = geomB.getStrA(null);

            return new ST_DistanceConstant(ga, gb);
        } else {
            return new ST_DistanceFunction(geomA, geomB);
        }
    }

    private static class ST_DistanceFunction extends DoubleFunction {
        private final Function geomA;
        private final Function geomB;

        public ST_DistanceFunction(Function geomA, Function geomB) {
            this.geomA = geomA;
            this.geomB = geomB;
        }

        @Override
        public double getDouble(Record rec) {
            final CharSequence a = geomA.getStrA(rec);
            final CharSequence b = geomB.getStrA(rec);

            if(a == null || b == null) {
                return Double.NaN;
            }
            WKTReader wktReader = new WKTReader();
            wktReader.setIsOldJtsCoordinateSyntaxAllowed(false);
            try {
                final Geometry ga = wktReader.read(String.valueOf(a));
                final Geometry gb = wktReader.read(String.valueOf(b));
                final double d = ga.distance(gb);
                return d;
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(geomB).val(',').val(geomA).val(')');
        }
    }

    private static class ST_DistanceConstant extends DoubleFunction {
        private final CharSequence geomA;
        private final CharSequence geomB;

        public ST_DistanceConstant(CharSequence geomA, CharSequence geomB) {
            this.geomA = geomA;
            this.geomB = geomB;
        }

        @Override
        public double getDouble(Record rec) {
            final CharSequence a = geomA;
            final CharSequence b = geomB;

            if(a == null || b == null) {
                return Double.NaN;
            }
            WKTReader wktReader = new WKTReader();
            wktReader.setIsOldJtsCoordinateSyntaxAllowed(false);
            try {
                final Geometry ga = wktReader.read(String.valueOf(a));
                final Geometry gb = wktReader.read(String.valueOf(b));
                final double d = ga.distance(gb);
                return d;
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(geomB).val(',').val(geomA).val(')');
        }
    }
}

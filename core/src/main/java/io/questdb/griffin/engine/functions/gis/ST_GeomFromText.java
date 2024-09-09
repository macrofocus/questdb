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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

import java.sql.SQLException;

public class ST_GeomFromText implements FunctionFactory {
    private static final String SYMBOL = "st_geomfromtext";
    private static final String SIGNATURE = SYMBOL + "(Ø)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function kktArg = args.get(0);

        return new ST_GeomFromTextFunction(kktArg);
    }

    private static class ST_GeomFromTextFunction extends StrFunction {
        private final Function wkt;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        public ST_GeomFromTextFunction(Function wkt) {
            this.wkt = wkt;
        }

        @Contract(pure = true)
        @Override
        public @Nullable CharSequence getStrA(Record rec) {
            final CharSequence value = wkt.getStrA(rec);

            if(value == null) {
                return null;
            }
            WKTReader wktReader = new WKTReader();
            wktReader.setIsOldJtsCoordinateSyntaxAllowed(false);
            try {
                final Geometry geom = wktReader.read(String.valueOf(value));
                WKTWriter wktWriter = new WKTWriter();
                String wkt = wktWriter.write(geom);
                sinkA.clear();
                sinkA.put(wkt);
                return sinkA;
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return null;
        }


        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SYMBOL).val('(').val(wkt).val(',');
        }
    }
}

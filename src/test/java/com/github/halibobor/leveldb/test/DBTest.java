/*
 * Copyright (C) 2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of FuseSource Corp. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.halibobor.leveldb.test;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import junit.framework.TestCase;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.junit.Assert;
import org.junit.Test;

/**
 * A Unit test for the DB class implementation.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class DBTest extends TestCase {

    File getTestDirectory(String name) throws IOException {
        File rc = new File(new File("test-data"), name);
        factory.destroy(rc, new Options().createIfMissing(true));
        rc.mkdirs();
        return rc;
    }


    @Test
    public void testWriteBatch() throws IOException, DBException {

        Options options = new Options().createIfMissing(true);

        File path = getTestDirectory(getName());
        DB db = factory.open(path, options);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 500 ; i++) {
            try (WriteBatch batch = db.createWriteBatch()) {
                db.delete(bytes((i - 30 ) + ""));
                StringBuilder s = new StringBuilder();
                for (int j = 0; j < 1024 * 1024 * 3; j++) {
                    s.append((char)(new Random().nextInt(128)));
                }
                batch.put(bytes(i +""),bytes(s.toString()));
                db.write(batch);
                db.compactRange(null, null);
                statProperty(db, i);
            }
        }

        int c = 0;
        try (DBIterator iterator = db.iterator()){
            for (iterator.seekToFirst();iterator.hasNext();iterator.next()) {
                c++;
            }
        }
        Assert.assertEquals(30, c);
        long e = System.currentTimeMillis();
        System.out.println( e -start);
        db.close();
        factory.destroy(path, new Options());
    }

    private void statProperty(DB  db, int i) {
        try {
            List<String > stats = Arrays.stream(db.getProperty("leveldb.stats")
                .split("\n")).skip(3).collect(Collectors.toList());
            double total = 0;
            for (String stat : stats) {
                String[] tmp = stat.trim().replaceAll(" +", ",").split(",");
                total += Double.parseDouble(tmp[2]);
            }
            System.out.println(i + ":" + total + " M");
        } catch (Exception e) {

        }
    }

}

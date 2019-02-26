package edu.cmu.cs.cloud;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Usage:
 * mvn test
 *
 * <p>You should pass all the provided test cases before you make any submission.
 *
 * <p>Feel free to add more test cases.
 */
class RedisTest {

    /**
     * Used Java Reflection to access {@link Redis#store} which is private.
     *
     * @throws NoSuchFieldException   if a field with the specified name is
     *                                not found.
     * @throws IllegalAccessException if this {@code Field} object
     *                                is enforcing Java language access control
     *                                and the underlying field is either inaccessible or final.
     */
    @Test
    void type() throws NoSuchFieldException, IllegalAccessException {
        Redis redisClient = new Redis();

        // use reflection to access the private field
        Field field = redisClient.getClass().getDeclaredField("store");
        field.setAccessible(true);
        HashMap<String, Object> store = new HashMap<>();
        store.put("unknown", new Object());
        field.set(redisClient, store);

        assertEquals("OK", redisClient.set("mykey", "cloud"));
        assertEquals("string", redisClient.type("mykey"));

        redisClient.hset("hash", "myfield", "myvalue");
        assertEquals("hash", redisClient.type("hash"));

        redisClient.rpush("list", "myvalue1", "myvalue2", "myvalue3");
        assertEquals("list", redisClient.type("list"));

        assertEquals("none", redisClient.type("the key does not exist"));
        assertEquals("unknown", redisClient.type("unknown"));

    }

    @Test
    void checkType() {
        Redis redisClient = new Redis();
        assertEquals("OK", redisClient.set("mykey", "cloud"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> redisClient.checkType("mykey", "HASH"));
    }

    @Test
    void set() {
        Redis redisClient = new Redis();

        assertEquals("OK", redisClient.set("mykey", "cloud"));
        assertEquals("OK", redisClient.set("mykey", "cool"));
        assertEquals("OK", redisClient.set("secondkey", "yes"));
        assertNotEquals("Random", redisClient.set("thirdkey", "no"));
    }

    @Test
    void get() {
        Redis redisClient = new Redis();

        assertNull(redisClient.get("mykey"));

        redisClient.set("mykey", "cloud");
        assertNotNull(redisClient.get("mykey"));
        assertEquals("cloud", redisClient.get("mykey"));

        redisClient.set("mykey", "cool");
        assertNotEquals("cloud", redisClient.get("mykey"));
        assertEquals("cool", redisClient.get("mykey"));
    }

    @Test
    void del() {
        Redis redisClient = new Redis();

        assertEquals(0, redisClient.del("mykey"));

        redisClient.set("mykey1", "cloud");
        redisClient.set("mykey2", "cool");
        redisClient.set("mykey3", "awesome");
        assertEquals(2, redisClient.del("mykey1", "mykey3"));

        assertNotEquals(0, redisClient.del("mykey2"));
    }

    @Test
    void hset() {
        Redis redisClient = new Redis();

        assertEquals(1, redisClient.hset("myhash", "mykey1", "myval1"));
        assertEquals(1, redisClient.hset("myhash", "mykey2", "myval2"));
        assertEquals(0, redisClient.hset("myhash", "mykey1", "newval1"));
        assertNotEquals(1, redisClient.hset("myhash", "mykey2", "newval2"));
        assertNotEquals(0, redisClient.hset("myhash", "mykey3", "myval3"));
    }

    @Test
    void hget() {
        Redis redisClient = new Redis();

        assertNull(redisClient.hget("myhash", "mykey"));

        redisClient.hset("myhash", "mykey1", "myval1");
        assertNotNull(redisClient.hget("myhash", "mykey1"));
        assertEquals("myval1", redisClient.hget("myhash", "mykey1"));

        redisClient.hset("myhash", "mykey1", "newval1");
        assertNotEquals("myval1", redisClient.hget("myhash", "mykey1"));
        assertEquals("newval1", redisClient.hget("myhash", "mykey1"));

        assertNull(redisClient.hget("myhash", "mykey2"));
    }

    @Test
    void hgetall() {
        Redis redisClient = new Redis();

        List<String> expectedList = new LinkedList<>();
        assertThat(expectedList, equalTo(redisClient.hgetall("myhash")));

        redisClient.hset("myhash", "mykey1", "myval1");
        redisClient.hset("myhash", "mykey2", "myval2");

        assertThat(expectedList, not(redisClient.hgetall("myhash")));
        expectedList.add("mykey1");
        expectedList.add("myval1");
        expectedList.add("mykey2");
        expectedList.add("myval2");
        assertThat(expectedList, containsInAnyOrder(redisClient.hgetall("myhash").toArray()));
    }

    @Test
    void llen() {
        Redis redisClient = new Redis();

        assertEquals(0, redisClient.llen("mylist"));

        redisClient.rpush("mylist", "val1");
        assertNotEquals(0, redisClient.llen("mylist"));
        assertEquals(1, redisClient.llen("mylist"));

        redisClient.rpush("mylist", "val2");
        assertNotEquals(1, redisClient.llen("mylist"));
        assertEquals(2, redisClient.llen("mylist"));

        redisClient.rpop("mylist");
        assertNotEquals(2, redisClient.llen("mylist"));
        assertEquals(1, redisClient.llen("mylist"));

    }

    @Test
    void rpush() {
        Redis redisClient = new Redis();

        assertEquals(1, redisClient.rpush("mylist", "val1"));
        assertEquals(2, redisClient.rpush("mylist", "val2"));
        redisClient.rpop("mylist");
        assertEquals(2, redisClient.rpush("mylist", "val2"));
        assertNotEquals(4, redisClient.rpush("mylist", "val3"));
    }

    @Test
    void rpop() {
        Redis redisClient = new Redis();

        assertNull(redisClient.rpop("mylist"));

        redisClient.rpush("mylist", "val1");
        assertNotNull(redisClient.rpop("mylist"));

        redisClient.rpush("mylist", "val1");
        assertEquals("val1", redisClient.rpop("mylist"));

        redisClient.rpush("mylist", "val1");
        redisClient.rpush("mylist", "val2");
        assertEquals("val2", redisClient.rpop("mylist"));
    }
}
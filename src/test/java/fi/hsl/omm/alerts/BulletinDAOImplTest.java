package fi.hsl.omm.alerts;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class BulletinDAOImplTest {
    static final String MAX = Long.toString(Long.MAX_VALUE);

    @Test
    public void testSimpleCSVParsing() {
        List<Long> one = BulletinDAOImpl.parseListFromCommaSeparatedString(MAX);
        assertEquals(1, one.size());
        assertEquals((Long) Long.MAX_VALUE, one.get(0));

        List<Long> four = BulletinDAOImpl.parseListFromCommaSeparatedString("0,1,2,3");
        Long[] expectedFour = {0L, 1L, 2L, 3L};
        assertArrayEquals(expectedFour, four.toArray());
    }

    @Test
    public void testNullCSVParsing() {
        List<Long> empty = BulletinDAOImpl.parseListFromCommaSeparatedString("");
        assertEquals(0, empty.size());

        List<Long> nullInput = BulletinDAOImpl.parseListFromCommaSeparatedString(null);
        assertEquals(0, nullInput.size());
    }

    @Test
    public void testExtraCommaParsing() {
        List<Long> four = BulletinDAOImpl.parseListFromCommaSeparatedString("0,1,2,3,");
        Long[] expectedFour = {0L, 1L, 2L, 3L};
        assertArrayEquals(expectedFour, four.toArray());
    }
}

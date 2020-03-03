package fi.hsl.omm.alerts.models;

import org.junit.jupiter.api.*;

import java.time.*;
import java.util.ArrayList;
import java.util.*;
import java.util.stream.*;

import static java.util.Arrays.*;
import static org.junit.jupiter.api.Assertions.*;

public class BulletinTest {
    @Test
    public void testEqualsWithNullCheck() {
        Long l1 = 1L;
        Long l2 = 2L;
        assertTrue(Bulletin.equalsWithNullCheck(l1, l1));
        assertFalse(Bulletin.equalsWithNullCheck(l1, l2));
        assertFalse(Bulletin.equalsWithNullCheck(l2, l1));
        assertFalse(Bulletin.equalsWithNullCheck(null, l1));
        assertFalse(Bulletin.equalsWithNullCheck(l2, null));
        assertTrue(Bulletin.equalsWithNullCheck(null, null));
    }

    @Test
    public void testEqualsWithNullCheckWithLists() {
        List<Long> multipleInOrder = new ArrayList<Long>() {{
            add(new Long(1L));
            add(new Long(2L));
            add(new Long(3L));
        }};
        List<Long> onlyOne = new ArrayList<Long>() {{
            add(new Long(1L));
        }};
        List<Long> multipleInReverseOrder = new ArrayList<Long>() {{
            add(new Long(3L));
            add(new Long(2L));
            add(new Long(1L));
        }};

        assertTrue(Bulletin.equalsWithNullCheck(multipleInOrder, multipleInOrder));//same instance
        assertTrue(Bulletin.equalsWithNullCheck(multipleInOrder, new ArrayList(multipleInOrder)));//new instance
        assertTrue(Bulletin.equalsWithNullCheck(new LinkedList<>(multipleInOrder), new LinkedList<>(multipleInOrder)));//new instances

        assertFalse(Bulletin.equalsWithNullCheck(multipleInOrder, multipleInReverseOrder));
        assertFalse(Bulletin.equalsWithNullCheck(multipleInOrder, onlyOne));
    }

    @Test
    public void testEqualsWithNullCheckWithLocalDateTimes() {
        LocalDateTime utcEpoch1 = LocalDateTime.ofEpochSecond(1542696000L, 0, ZoneOffset.UTC);
        LocalDateTime utcEpoch2 = LocalDateTime.ofEpochSecond(1542696001L, 0, ZoneOffset.UTC);
        LocalDateTime eetEpoch = LocalDateTime.ofEpochSecond(1542696000L, 0, ZoneOffset.of("+02"));

        assertTrue(Bulletin.equalsWithNullCheck(utcEpoch1, utcEpoch1));//same instance
        assertTrue(Bulletin.equalsWithNullCheck(utcEpoch1, LocalDateTime.of(utcEpoch1.toLocalDate(), utcEpoch1.toLocalTime())));//new instance
        assertTrue(Bulletin.equalsWithNullCheck(eetEpoch, LocalDateTime.of(eetEpoch.toLocalDate(), eetEpoch.toLocalTime())));//new instance. non-utc zone

        assertFalse(Bulletin.equalsWithNullCheck(utcEpoch1, utcEpoch2));
        assertFalse(Bulletin.equalsWithNullCheck(utcEpoch1, eetEpoch));
    }

    @Test
    public void testCategoryToEnum() {
        List<Bulletin.Category> all = asList(Bulletin.Category.values());
        assertEquals(32, all.size());
        for (Bulletin.Category c : all) {
            assertNotNull(c.toCategory());
        }
    }

    @Test
    public void testImpactToEnum() {
        List<Bulletin.Impact> all = asList(Bulletin.Impact.values());
        assertEquals(14, all.size());
        for (Bulletin.Impact i : all) {
            assertNotNull(i.toImpact());
        }
    }

    @Test
    public void testPriorityToEnum() {
        List<Bulletin.Priority> all = asList(Bulletin.Priority.values());
        assertEquals(3, all.size());
        for (Bulletin.Priority i : all) {
            assertNotNull(i.toPriority());
        }
    }

    @Test
    public void testLanguageCodes() {
        Set<String> languageCodes = Arrays.stream(Bulletin.Language.values()).map(Bulletin.Language::toString).collect(Collectors.toSet());
        assertEquals(3, languageCodes.size());
        assertTrue(languageCodes.contains("fi"));
        assertTrue(languageCodes.contains("en"));
        assertTrue(languageCodes.contains("sv"));

    }
}

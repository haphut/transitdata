package fi.hsl;

import org.junit.*;
import org.slf4j.*;

import java.util.*;
import java.util.regex.*;

import static junit.framework.TestCase.*;

public class ITMockDataUtilsTest {
    static final Logger logger = LoggerFactory.getLogger(ITMockDataUtilsTest.class);

    @Test
    public void testRouteNameGenerator() {
        for (int n = 0; n < 10000; n++) {
            String route = MockDataUtils.generateValidRouteName();
            //logger.info(route);
            Pattern p = Pattern.compile(MockDataUtils.JORE_ROUTE_NAME_REGEX);
            Matcher matcher = p.matcher(route);
            assertTrue("testing route name " + route, matcher.matches());
        }
    }

    @Test
    public void testJoreIdGenerator() {
        long id = MockDataUtils.generateValidJoreId();
        assertTrue(id > 999999999999999L);
        assertTrue(id < 10000000000000000L);
        logger.info("id " + id);
    }

    @Test
    public void testStopSeqListGenerator() {
        final int length = 5;
        List<Integer> seq = MockDataUtils.generateStopSequenceList(length);
        assertEquals(length, seq.size());

        Integer[] expected = {0, 1, 2, 3, 4};
        assertTrue(Arrays.equals(expected, seq.toArray(new Integer[0])));
    }
}

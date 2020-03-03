package fi.hsl.omm.alerts.models;

import fi.hsl.common.transitdata.proto.*;
import fi.hsl.omm.alerts.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

public class AlertStateTest {

    List<Long> ids = Arrays.asList(1L, 2L, 3L);
    List<Long> idsInReverse = Arrays.asList(3L, 2L, 1L);

    @Test
    public void testSorting() throws Exception {
        List<Bulletin> ordered = BulletinDAOMock.newMockDAO(ids).getActiveBulletins();
        List<Bulletin> reversed = BulletinDAOMock.newMockDAO(idsInReverse).getActiveBulletins();

        assertNotEquals(ordered, reversed);
        assertEquals(ordered, AlertState.asSorted(reversed));
    }

    @Test
    public void testSortingReturnsNewList() throws Exception {
        List<Bulletin> ordered = BulletinDAOMock.newMockDAO(ids).getActiveBulletins();
        List<Bulletin> reversed = BulletinDAOMock.newMockDAO(idsInReverse).getActiveBulletins();

        List<Bulletin> sortedReversed = AlertState.asSorted(reversed);
        assertEquals(ordered, sortedReversed);

        assertEquals(reversed.size(), sortedReversed.size());
        reversed.remove(0);
        assertEquals(reversed.size() + 1, sortedReversed.size());

    }

    @Test
    public void testEqualsForSameLists() throws Exception {
        List<Bulletin> bulletins = readDefaultTestBulletins();
        AlertState first = new AlertState(bulletins);

        ArrayList<Bulletin> copy = new ArrayList<>(bulletins);
        AlertState second = new AlertState(copy);

        assertEquals(first, second);
    }

    private List<Bulletin> readDefaultTestBulletins() throws Exception {
        MockOmmConnector connector = MockOmmConnector.newInstance("2019_05_alert_dump.tsv");
        return connector.getBulletinDAO().getActiveBulletins();
    }

    @Test
    public void testEqualsForOrderingChanged() throws Exception {
        //We don't care about the order, just about the actual state.
        List<Bulletin> bulletins = readDefaultTestBulletins();
        AlertState first = new AlertState(bulletins);

        ArrayList<Bulletin> shuffled = new ArrayList<>(bulletins);
        Collections.shuffle(shuffled);
        ArrayList<Bulletin> copyOfShuffled = new ArrayList<>(shuffled);

        AlertState second = new AlertState(shuffled);

        assertNotEquals(bulletins, shuffled); //Ordering different -> Lists not equal
        assertEquals(first, second); // we don't care about ordering within the state
        assertEquals(shuffled, copyOfShuffled); // AlertState.equals should not change the underlying lists
    }

    @Test
    public void testEqualsWhenOneRemoved() throws Exception {
        List<Bulletin> bulletins = readDefaultTestBulletins();
        AlertState first = new AlertState(bulletins);

        ArrayList<Bulletin> copy = new ArrayList<>(bulletins);
        copy.remove(0);
        AlertState second = new AlertState(copy);

        assertNotEquals(first, second);
    }

    @Test
    public void testEqualsWhenSomethingChanged() throws Exception {
        List<Bulletin> firstBulletins = readDefaultTestBulletins();
        final AlertState first = new AlertState(firstBulletins);

        final List<Bulletin> secondBulletins = readDefaultTestBulletins();

        //Change fields one at the time and make sure each change gets noticed
        AlertState modified;

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setImpact(Bulletin.Impact.DISRUPTION_ROUTE);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setCategory(Bulletin.Category.ROAD_CLOSED);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setPriority(Bulletin.Priority.SEVERE);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setId(404L);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setLastModified(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setValidFrom(Optional.of(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())));
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setValidTo(Optional.of(LocalDateTime.ofInstant(Instant.now(), ZoneId.systemDefault())));
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setAffectsAllRoutes(!bulletin.isAffectsAllRoutes());
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.setAffectsAllStops(!bulletin.isAffectsAllStops());
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.getAffectedStopGids().add(123456789L);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            bulletin.getAffectedLineGids().add(987654321L);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            InternalMessages.Bulletin.Translation changedTranslation = bulletin.getDescriptions().get(0).toBuilder().setText("changing this").build();
            bulletin.getDescriptions().remove(0);
            bulletin.getDescriptions().add(0, changedTranslation);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            InternalMessages.Bulletin.Translation changedTranslation = bulletin.getTitles().get(0).toBuilder().setText("changing the header").build();
            bulletin.getTitles().remove(0);
            bulletin.getTitles().add(0, changedTranslation);
            return bulletin;
        });
        assertNotEquals(first, modified);

        modified = createModifiedAlertState(secondBulletins, bulletin -> {
            InternalMessages.Bulletin.Translation changedTranslation = bulletin.getUrls().get(0).toBuilder().setText("changing the url").build();
            bulletin.getUrls().remove(0);
            bulletin.getUrls().add(0, changedTranslation);
            return bulletin;
        });
        assertNotEquals(first, modified);

        //As last let's validate our test method. State should be equal if lambda does nothing
        AlertState unchanged = createModifiedAlertState(secondBulletins, bulletin -> bulletin);
        assertEquals(first, unchanged);

    }

    private AlertState createModifiedAlertState(final List<Bulletin> secondBulletins, Function<Bulletin, Bulletin> transformer) {
        final int indexToModify = 0;

        List<Bulletin> copyList = new ArrayList<>(secondBulletins);

        final Bulletin copyToModify = new Bulletin(copyList.get(indexToModify));
        Bulletin modified = transformer.apply(copyToModify);
        copyList.set(indexToModify, modified);

        return new AlertState(copyList);
    }
}

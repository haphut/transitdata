package fi.hsl.omm.alerts;


import fi.hsl.common.transitdata.proto.*;
import fi.hsl.omm.alerts.models.*;

import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class BulletinDAOMock implements BulletinDAO {
    List<Bulletin> mockData;

    BulletinDAOMock(List<Bulletin> data) {
        mockData = data;
    }

    public static Bulletin parseBulletinFromTsv(String line) {
        String[] split = line.split("\t");
        int index = 0;
        Bulletin b = new Bulletin();
        b.setId(Long.parseLong(split[index++]));
        b.setImpact(Bulletin.Impact.fromString(split[index++]));
        b.setCategory(Bulletin.Category.fromString(split[index++]));
        b.setLastModified(DAOImplBase.parseOmmLocalDateTime(split[index++]));
        b.setValidFrom(DAOImplBase.parseNullableOmmLocalDateTime(split[index++]));
        b.setValidTo(DAOImplBase.parseNullableOmmLocalDateTime(split[index++]));
        b.setAffectsAllRoutes(Boolean.parseBoolean(split[index++]));
        b.setAffectsAllStops(Boolean.parseBoolean(split[index++]));
        b.setAffectedLineGids(BulletinDAOImpl.parseListFromCommaSeparatedString(split[index++]));
        b.setAffectedStopGids(BulletinDAOImpl.parseListFromCommaSeparatedString(split[index++]));
        Optional<Bulletin.Priority> maybePriority = Bulletin.Priority.fromInt(Integer.parseInt(split[index++]));
        if (maybePriority.isPresent()) {
            b.setPriority(maybePriority.get());
        }

        String titleFi = split[index++];
        String titleSv = split[index++];
        String titleEn = split[index++];

        String textFi = split[index++];
        String textSv = split[index++];
        String textEn = split[index++];

        String urlFi = split[index++];
        String urlSv = split[index++];
        String urlEn = split[index++];

        b.setTitles(createTranslatedString(titleFi, titleSv, titleEn));
        b.setDescriptions(createTranslatedString(textFi, textSv, textEn));
        b.setUrls(createTranslatedString(urlFi, urlSv, urlEn));
        return b;
    }

    private static List<InternalMessages.Bulletin.Translation> createTranslatedString(String fi, String sv, String en) {
        List<InternalMessages.Bulletin.Translation> translations = new ArrayList<>();

        InternalMessages.Bulletin.Translation translationFi = InternalMessages.Bulletin.Translation.newBuilder()
                .setText(fi)
                .setLanguage(Bulletin.Language.fi.toString()).build();
        translations.add(translationFi);

        if (sv != null) {
            InternalMessages.Bulletin.Translation translationSv = InternalMessages.Bulletin.Translation.newBuilder()
                    .setText(sv)
                    .setLanguage(Bulletin.Language.sv.toString()).build();
            translations.add(translationSv);
        }
        if (en != null) {
            InternalMessages.Bulletin.Translation translationEn = InternalMessages.Bulletin.Translation.newBuilder()
                    .setText(en)
                    .setLanguage(Bulletin.Language.en.toString()).build();
            translations.add(translationEn);
        }

        return translations;
    }

    public static BulletinDAOMock newMockDAO(List<Long> ids) {
        List<Bulletin> mocks = ids.stream()
                .map(BulletinDAOMock::newMockBulletin)
                .collect(Collectors.toList());
        return new BulletinDAOMock(mocks);
    }

    public static Bulletin newMockBulletin(long id) {
        Bulletin b = new Bulletin();
        b.setId(id);
        b.setCategory(Bulletin.Category.TRAFFIC_ACCIDENT);
        b.setImpact(Bulletin.Impact.DELAYED);
        return b;
    }

    @Override
    public List<Bulletin> getActiveBulletins() throws SQLException {
        return mockData;
    }

}

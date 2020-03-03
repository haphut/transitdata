package fi.hsl.tripupdate.departure;

import fi.hsl.common.pubtrans.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.proto.*;

import java.sql.*;
import java.util.*;

public class DepartureHandler extends PubtransTableHandler {

    private static final TransitdataSchema schema;

    static {
        int defaultVersion = PubtransTableProtos.ROIDeparture.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiDeparture, Optional.of(defaultVersion));
    }

    public DepartureHandler(PulsarApplicationContext context) {
        super(context, TransitdataProperties.ProtobufSchema.PubtransRoiDeparture);
    }

    @Override
    protected String getTimetabledDateTimeColumnName() {
        return "TimetabledEarliestDateTime";
    }

    @Override
    protected TransitdataSchema getSchema() {
        return schema;
    }

    @Override
    protected byte[] createPayload(ResultSet resultSet, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) throws SQLException {
        PubtransTableProtos.ROIDeparture.Builder departureBuilder = PubtransTableProtos.ROIDeparture.newBuilder();
        departureBuilder.setSchemaVersion(departureBuilder.getSchemaVersion());
        departureBuilder.setCommon(common);
        departureBuilder.setTripInfo(tripInfo);
        if (resultSet.getBytes("HasDestinationDisplayId") != null)
            departureBuilder.setHasDestinationDisplayId(resultSet.getLong("HasDestinationDisplayId"));
        if (resultSet.getBytes("HasDestinationStopAreaGid") != null)
            departureBuilder.setHasDestinationStopAreaGid(resultSet.getLong("HasDestinationStopAreaGid"));
        if (resultSet.getBytes("HasServiceRequirementId") != null)
            departureBuilder.setHasServiceRequirementId(resultSet.getLong("HasServiceRequirementId"));
        PubtransTableProtos.ROIDeparture departure = departureBuilder.build();
        return departure.toByteArray();
    }

}

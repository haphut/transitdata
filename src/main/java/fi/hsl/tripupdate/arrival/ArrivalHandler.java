package fi.hsl.tripupdate.arrival;

import fi.hsl.common.pubtrans.*;
import fi.hsl.common.pulsar.*;
import fi.hsl.common.transitdata.*;
import fi.hsl.common.transitdata.proto.*;

import java.sql.*;
import java.util.*;

public class ArrivalHandler extends PubtransTableHandler {

    private static final TransitdataSchema schema;

    static {
        int defaultVersion = PubtransTableProtos.ROIArrival.newBuilder().getSchemaVersion();
        schema = new TransitdataSchema(TransitdataProperties.ProtobufSchema.PubtransRoiArrival, Optional.of(defaultVersion));
    }

    public ArrivalHandler(PulsarApplicationContext context) {
        super(context, TransitdataProperties.ProtobufSchema.PubtransRoiArrival);
    }

    @Override
    protected String getTimetabledDateTimeColumnName() {
        return "TimetabledLatestDateTime";
    }

    @Override
    protected TransitdataSchema getSchema() {
        return schema;
    }

    @Override
    protected byte[] createPayload(ResultSet resultSet, PubtransTableProtos.Common common, PubtransTableProtos.DOITripInfo tripInfo) {
        PubtransTableProtos.ROIArrival.Builder arrivalBuilder = PubtransTableProtos.ROIArrival.newBuilder();
        arrivalBuilder.setSchemaVersion(arrivalBuilder.getSchemaVersion());
        arrivalBuilder.setCommon(common);
        arrivalBuilder.setTripInfo(tripInfo);
        PubtransTableProtos.ROIArrival arrival = arrivalBuilder.build();
        return arrival.toByteArray();
    }

}

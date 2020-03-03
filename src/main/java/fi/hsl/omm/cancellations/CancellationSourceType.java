package fi.hsl.omm.cancellations;

public enum CancellationSourceType {
    FROM_PAST,
    FROM_NOW;

    public static CancellationSourceType fromString(String cancellationSourceType) {
        if ("PAST".equals(cancellationSourceType))
            return FROM_PAST;
        else if ("NOW".equals(cancellationSourceType))
            return FROM_NOW;
        return null;
    }

    public String toString() {
        switch (this) {
            case FROM_PAST:
                return "fromPast";
            case FROM_NOW:
                return "fromNow";
        }
        return "";
    }
}

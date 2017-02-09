package org.pbccrc.zsls.store.jdbc.builder;

import org.springframework.util.StringUtils;

/**
 */
public enum OrderByType {
    DESC, ASC;

    public static OrderByType convert(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return OrderByType.valueOf(value);
    }

}

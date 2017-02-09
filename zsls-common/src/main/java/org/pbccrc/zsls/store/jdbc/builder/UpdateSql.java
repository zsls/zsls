package org.pbccrc.zsls.store.jdbc.builder;

import org.apache.log4j.Logger;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.store.jdbc.SQLFormatter;
import org.pbccrc.zsls.store.jdbc.SqlTemplate;
import org.pbccrc.zsls.store.jdbc.Transaction;
import org.pbccrc.zsls.store.jdbc.utils.JdbcConstants;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Robert HG (254963746@qq.com) on 3/9/16.
 */
public class UpdateSql {

    private static final Logger LOGGER = Logger.getLogger(UpdateSql.class);

    private SqlTemplate sqlTemplate;
    private StringBuilder sql = new StringBuilder();
    private List<Object> params = new LinkedList<Object>();

    public UpdateSql(SqlTemplate sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }
    
    private Transaction trans;
   	public UpdateSql inTransition(Transaction trans) {
   		this.trans = trans;
   		return this;
   	}

    public UpdateSql update() {
        sql.append("UPDATE ");
        return this;
    }

    public UpdateSql table(String table) {
        sql.append(JdbcConstants.TNAME_B_DEFAULT).append(table).append(JdbcConstants.TNAME_A_DEFAULT);
        return this;
    }

    public UpdateSql set(String column, Object value) {
        if (params.size() > 0) {
            sql.append(",");
        } else {
            sql.append(" SET ");
        }
        sql.append(JdbcConstants.TNAME_B_DEFAULT).append(column).append(JdbcConstants.TNAME_A_DEFAULT).append(" = ? ");
        params.add(value);
        return this;
    }

    public UpdateSql setOnNotNull(String column, Object value) {
        if (value == null) {
            return this;
        }
        return set(column, value);
    }

    public UpdateSql where() {
        sql.append(" WHERE ");
        return this;
    }

    public UpdateSql whereSql(WhereSql whereSql) {
        sql.append(whereSql.getSQL());
        params.addAll(whereSql.params());
        return this;
    }

    public UpdateSql where(String condition, Object value) {
        sql.append(" WHERE ").append(condition);
        params.add(value);
        return this;
    }

    public UpdateSql and(String condition, Object value) {
        sql.append(" AND ").append(condition);
        params.add(value);
        return this;
    }

    public UpdateSql or(String condition, Object value) {
        sql.append(" OR ").append(condition);
        params.add(value);
        return this;
    }

    public UpdateSql and(String condition) {
        sql.append(" AND ").append(condition);
        return this;
    }

    public UpdateSql or(String condition) {
        sql.append(" OR ").append(condition);
        return this;
    }

    public UpdateSql andOnNotNull(String condition, Object value) {
        if (value == null) {
            return this;
        }
        return and(condition, value);
    }

    public UpdateSql orOnNotNull(String condition, Object value) {
        if (value == null) {
            return this;
        }
        return or(condition, value);
    }

    public UpdateSql andOnNotEmpty(String condition, String value) {
        if (!StringUtils.hasText(value)) {
            return this;
        }
        return and(condition, value);
    }

    public UpdateSql orOnNotEmpty(String condition, String value) {
        if (!StringUtils.hasText(value)) {
            return this;
        }
        return or(condition, value);
    }

    public UpdateSql andBetween(String column, Object start, Object end) {

        if (start == null && end == null) {
            return this;
        }

        if (start != null && end != null) {
            sql.append(" ADN (").append(column).append(" BETWEEN ? AND ? ").append(")");
            params.add(start);
            params.add(end);
            return this;
        }

        if (start == null) {
            sql.append(column).append(" <= ? ");
            params.add(end);
            return this;
        }

        sql.append(column).append(" >= ? ");
        params.add(start);
        return this;
    }

    public UpdateSql orBetween(String column, Object start, Object end) {

        if (start == null && end == null) {
            return this;
        }

        if (start != null && end != null) {
            sql.append(" OR (").append(column).append(" BETWEEN ? AND ? ").append(")");
            params.add(start);
            params.add(end);
            return this;
        }

        if (start == null) {
            sql.append(column).append(" <= ? ");
            params.add(end);
            return this;
        }

        sql.append(column).append(" >= ? ");
        params.add(start);
        return this;
    }

    public int doUpdate() {
        String finalSQL = getSQL();
        try {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(SQLFormatter.format(finalSQL));
            }

            return sqlTemplate.update(finalSQL, trans, params.toArray());
        } catch (SQLException e) {
            throw new JdbcException("Update SQL Error:" + SQLFormatter.format(finalSQL), e);
        }
    }

    public String getSQL() {
        return sql.toString();
    }
}

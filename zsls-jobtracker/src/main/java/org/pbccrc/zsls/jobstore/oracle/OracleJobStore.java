package org.pbccrc.zsls.jobstore.oracle;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.pbccrc.zsls.api.client.old.IScheduleUnit;
import org.pbccrc.zsls.config.DbConfig;
import org.pbccrc.zsls.exception.store.JdbcException;
import org.pbccrc.zsls.jobstore.JdbcJobStore;
import org.pbccrc.zsls.jobstore.mysql.Handlers.BatchUnitHandler;
import org.pbccrc.zsls.jobstore.mysql.Handlers.NumberHandler;
import org.pbccrc.zsls.store.jdbc.Transaction;
import org.pbccrc.zsls.store.jdbc.TransactionFactory;
import org.pbccrc.zsls.store.jdbc.builder.DropTableSql;
import org.pbccrc.zsls.store.jdbc.builder.InsertSql;
import org.pbccrc.zsls.store.jdbc.builder.OrderByType;
import org.pbccrc.zsls.store.jdbc.builder.SelectSql;
import org.pbccrc.zsls.tasks.rt.RTJobFlow;
import org.pbccrc.zsls.tasks.rt.RTJobId;
import org.pbccrc.zsls.utils.JsonSerilizer;

public class OracleJobStore extends JdbcJobStore {
	final String SEQUENCE = "SEQ_";
	final String NEXT_VALUE = ".NEXTVAL";
	final String CUR_VALUE = ".CURRVAL";
	
	final String ORACLE_DUAL = "DUAL";
	final String ORACLE_ALL_SEQUENCE = "All_Sequences";
	final String ORACLE_USER_TABLES = "USER_TABLES";
	final String ORACLE_TABLE_NAME = "TABLE_NAME";
	
	final String ORACLE_ROWNUM = "ROWNUM";
	final String ORACLE_ROWNUM_ALIAS = "RN_ALIAS";
	
	private NumberHandler numberHandler = new NumberHandler();
	
	public OracleJobStore(DbConfig config) {
		super(config);
		if (!tableExist(JdbcJobStore.TBLNAME_QJOB))
			createTable(readSqlFile("sql/oracle/jobstore_create_quartz_jobs.sql"));
		if (!tableExist(JdbcJobStore.TBLNAME_QTASK))
			createTable(readSqlFile("sql/oracle/jobstore_create_quartz_tasks.sql"));
		if (!tableExist(JdbcJobStore.TBLNAME_QTASK_PARAM))
			createTable(readSqlFile("sql/oracle/jobstore_create_quartz_tasksparam.sql"));
	}
	
	@Override
	public boolean initForDomain(String domain) {
		try {
			if (!tableExist(getUnitTable(domain))) {
				createSequenceIfNotExist(SEQUENCE + domain);
				createTable(readSqlFile("sql/oracle/jobstore_create_units.sql", domain));
			}
			if (!tableExist(getTaskTable(domain)))
				createTable(readSqlFile("sql/oracle/jobstore_create_tasks.sql", domain));	
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	@Override
	public List<RTJobFlow> fetchUnitsWithWindow(String domain, RTJobId unitid, int windowsize) {
		List<RTJobFlow> uList = new SelectSql(getSqlTemplate())
							.select()
							.all()
							.from()
							.table(getUnitTable(domain))
							.where(COL_UNIT_ID + " >= ?", unitid.getId())
							.and("ROWNUM <= " + windowsize)
							.list(new BatchUnitHandler());
		return uList;
	}
	
	protected SelectSql sqlSelectWhereDateMatch(SelectSql sql, Date date) {
		String condition = "TO_CHAR(" + COL_UNIT_CTIME + ",'YYYY-MM-DD') = ?";
		String val = new java.sql.Date(date.getTime()).toString();
		sql.where(condition, val);
		return sql;
	}
	
	private boolean tableExist(String table) {
		Long ret = new SelectSql(getSqlTemplate())
					.select()
					.columns("COUNT(*)")
					.from()
					.table(ORACLE_USER_TABLES)
					.where(ORACLE_TABLE_NAME + "='" + table.toUpperCase() + "'")
					.single(numberHandler);
		return ret > 0;
	}
	
	@Override
	protected long unitInstore(RTJobFlow unit, IScheduleUnit origUnit, Transaction trans) {
		long insertId = selectNextInsertId(trans, unit.getDomain());
		long preUnit = unit.getPreUnit() == null ? COL_UNIT_PREUNIT_DEF : unit.getPreUnit().getId();
		int ret = new InsertSql(getSqlTemplate())
						.inTransition(trans)
						.insert(getUnitTable(origUnit.domain))
						.columns(COL_UNIT_ID, COL_UNIT_PREUNIT, COL_UNIT_CONTENT, COL_UNIT_TASKNUM, COL_UNIT_SWIFTNUM)
						.values(insertId, preUnit, JsonSerilizer.serilize(origUnit), unit.getTaskNum(), unit.getSwiftNum())
						.doInsert();
		if (ret != 1)
			throw new JdbcException("insert " + JsonSerilizer.serilize(origUnit));
		return insertId;
	}
	
	protected long selectNextInsertId(Transaction trans, String domain) {
		Long ret = new SelectSql(getSqlTemplate())
						.inTransaction(trans)
						.select()
						.columns(SEQUENCE + domain + NEXT_VALUE)
						.from()
						.table(ORACLE_DUAL)//Oracle table dual
						.single(numberHandler);
		return ret.longValue();
	}
	
	private boolean sequenceExist(String sequence) {
		Long ret = new SelectSql(getSqlTemplate())
				.select()
				.columns("count(*)")
				.from()
				.table(ORACLE_ALL_SEQUENCE)//Oracle table dual
				.where("Sequence_name='" + sequence + "'")
				.single(numberHandler);
		if (ret == null)
				throw new JdbcException("can't query sequence with name " + sequence);
		return ret != 0;
	}
	
	private void createSequenceIfNotExist(String sequence) {
		String sql = "CREATE SEQUENCE " + sequence + " START WITH 1 INCREMENT BY 1";
		try {
			if (!sequenceExist(sequence))
				getSqlTemplate().createSequence(sql);
        } catch (Exception e) {
        	e.printStackTrace();
            throw new JdbcException("Create sequence error, sql=" + sql, e);
        }
	}

	@Override
	public RTJobId getLastUnit(String domain) {
		Long uid = new SelectSql(getSqlTemplate())
						.select()
						.columns("MAX(" + COL_UNIT_ID + ")")
						.from()
						.table(getUnitTable(domain))
						.single(numberHandler);
		if (uid == null)
			return null;
		return new RTJobId(uid.longValue());
	}

	@Override
	protected Object transformTime(Date date) {
		java.sql.Timestamp time = new java.sql.Timestamp(date.getTime() / 1000 * 1000);
		return time;
	}

	@Override
	public boolean cleanDomain(String domain) {
		Transaction trans = TransactionFactory.getTransaction();
		try {
			String unitTable = getUnitTable(domain);
			if (tableExist(unitTable))
				dropTable(trans, unitTable);
			String taskTable = getTaskTable(domain);
			if (tableExist(taskTable))
				dropTable(trans, taskTable);
			String sequence = SEQUENCE + domain;
			if (sequenceExist(sequence))
				deleteSequence(trans, domain);
			trans.commit();
		} catch (Exception e) {
			e.printStackTrace();
			try {
				trans.rollback();
			} catch (SQLException ignore) {
			}
			return false;
		}
		return true;
	}
	
	private void dropTable(Transaction trans, String table) {
		 new DropTableSql(getSqlTemplate())
			.drop(table)
			.inTransaction(trans)
			.doDrop();
	}
	
	private void deleteSequence(Transaction trans, String domain) {
		String sql = "DROP SEQUENCE " + SEQUENCE + domain;
		try {
			getSqlTemplate().update(sql, trans);
		} catch (SQLException e) {
			throw new JdbcException(e);
		}
	}

	@Override
	protected SelectSql limitResultSet(String sql, int start, int end) {
		SelectSql sql_limit = new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(" ( " + sql + " ) t ")
				.where()
				.between(ORACLE_ROWNUM_ALIAS, start, end);
		return sql_limit;
	}

	@Override
	public List<RTJobFlow> fetchUnitsByDate(String domain, Date date, int start, int end) {
		String table = getUnitTable(domain);
		SelectSql sql = new SelectSql(getSqlTemplate())
					.select()
					.columns(table + ".*", ORACLE_ROWNUM + " " + ORACLE_ROWNUM_ALIAS)
					.from()
					.table(table);
		
		sql = date != null ? sqlSelectWhereDateMatch(sql, date) : sql;
		sql.orderBy().column(COL_UNIT_ID, OrderByType.DESC);
		SimpleDateFormat sd = new SimpleDateFormat("YYYY-MM-dd");
		
		String _sql = sql.getSQL();
		if (_sql.contains("?"))
			_sql = _sql.replaceFirst("\\?", "'" + sd.format(date) + "'");
		sql = limitResultSet(_sql, start, end);
		List<RTJobFlow> uList = sql.list(new BatchUnitHandler());
		return uList;
	}
	
}

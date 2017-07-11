package org.pbccrc.zsls.jobstore.mysql;

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
import org.pbccrc.zsls.utils.DateUtils;
import org.pbccrc.zsls.utils.JsonSerilizer;

public class MysqlJobStore extends JdbcJobStore {
	
	private NumberHandler lastInsertHandler = new NumberHandler();

	public MysqlJobStore(DbConfig config) {
		super(config);
		createTable(readSqlFile("sql/mysql/jobstore_create_quartz_jobs.sql"));
		createTable(readSqlFile("sql/mysql/jobstore_create_quartz_tasks.sql"));
		createTable(readSqlFile("sql/mysql/jobstore_create_quartz_tasksparam.sql"));
	}
	
	@Override
	public boolean initForDomain(String domain) {
		try {
			createTable(readSqlFile("sql/mysql/jobstore_create_units.sql", domain));
			createTable(readSqlFile("sql/mysql/jobstore_create_tasks.sql", domain));	
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	protected long unitInstore(RTJobFlow unit, IScheduleUnit origUnit, Transaction trans) {
		long preUnit = unit.getPreUnit() == null ? COL_UNIT_PREUNIT_DEF : unit.getPreUnit().getId();
		int ret = new InsertSql(getSqlTemplate())
						.inTransition(trans)
						.insert(getUnitTable(origUnit.domain))
						.columns(COL_UNIT_CONTENT, COL_UNIT_TASKNUM, COL_UNIT_PREUNIT, COL_UNIT_SWIFTNUM)
						.values(JsonSerilizer.serilize(origUnit), unit.getTaskNum(), preUnit, unit.getSwiftNum())
						.doInsert();
		if (ret != 1)
			throw new JdbcException("insert " + JsonSerilizer.serilize(origUnit));
		return selectLastInsertId(trans);
	}
	
	protected long selectLastInsertId(Transaction trans) {
		Long id = new SelectSql(getSqlTemplate())
						.inTransaction(trans)
						.select()
						.lastInsertId()
						.single(lastInsertHandler);
		return id.longValue();
	}
	
	@Override
	public List<RTJobFlow> fetchUnitsWithWindow(String domain, RTJobId unitid, int windowSize) {
		List<RTJobFlow> uList = new SelectSql(getSqlTemplate())
						.select()
						.all()
						.from()
						.table(getUnitTable(domain))
						.where(COL_UNIT_ID + " >= ?", unitid.getId())
						.limit(0, windowSize)
						.list(new BatchUnitHandler());
		return uList;
	}
	
	@Override
	public RTJobId getLastUnit(String domain) {
		Long uid = new SelectSql(getSqlTemplate())
						.select()
						.columns("max(" + COL_UNIT_ID +") as " + COL_UNIT_ID)
						.from()
						.table(getUnitTable(domain))
						.single();
		if (uid == null)
			return null;
		return new RTJobId(uid);
	}

	@Override
	protected SelectSql sqlSelectWhereDateMatch(SelectSql sql, Date date) {
		String condition = "DATE_FORMAT(" + COL_UNIT_CTIME + ",'%Y-%m-%d') = ?";
		Object var = new java.sql.Date(date.getTime());
		return sql.where(condition, var);
	}

	@Override
	protected Object transformTime(Date date) {
		Object jDate = DateUtils.format(date);
		return jDate;
	}

	@Override
	public boolean cleanDomain(String domain) {
		Transaction trans = TransactionFactory.getTransaction();
		try {
			dropTable(getUnitTable(domain), trans);
			dropTable(getTaskTable(domain), trans);
			trans.commit();
			return true;
		} catch(Exception e) {
			e.printStackTrace();
			try {
				trans.rollback();
			} catch (SQLException ignore) {
			}
			return false;
		}
		
	}
	private void dropTable(String table, Transaction trans) {
		new DropTableSql(getSqlTemplate())
		.dropIfExist(table)
		.inTransaction(trans)
		.doDrop();
	}

	@Override
	protected SelectSql limitResultSet(String sql, int start, int end) {
		SelectSql sql_limit = new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(" ( " + sql + " ) t ")
				.limit(start, end);
		return sql_limit;
	}

	@Override
	public List<RTJobFlow> fetchUnitsByDate(String domain, Date date, int start, int end) {
		SelectSql sql = new SelectSql(getSqlTemplate())
				.select()
				.all()
				.from()
				.table(getUnitTable(domain));
				
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

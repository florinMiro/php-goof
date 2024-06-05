package com.amfam.lrts.persistence;
import com.amfam.lrts.action.*;
import com.amfam.lrts.form.*;
import com.amfam.lrts.util.* ;
import java.sql.*;
import java.text.ParseException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.StringTokenizer;
import org.apache.logging.log4j.*;
/**
 * PWorker takes care of the detail of interacting
 * with the database.  It is tightly coupled through
 * the db calls with the database structure, and
 * has awareness of the business objects through its
 * more coursely grained methods like
 * mainEntryFormAdd. PWorker could be replaced
 * entirely, or could be set to work with another
 * database, or a different structure without having
 * to modify the PersistenceManager role.
 *
 * Methods  and class is intentionally kept at default visibility,
 * because this class is intended as a utility for the
 * Persistence package.  It is not desirable that this class
 * is usable outside of this package.
 * Creation date: (12/13/2002 1:23:10 PM)
 * @author: Chris A Pauer
 *
 *
 * Design Modification History:
 *
 * 2/10/2004 (Eric Baeverstad) Changed the visibility to public so that the methods can
 * be called directly.  This bypasses the design (per notes above), but the value of
 * encapsulation of PWorker does not seem to outweigh the risks imposed by
 * PersistenceManagerImpl using reflection to derrive the method calls (a compile error
 * is much cleaner way of catching mis-named methods/classes).  Although the practice was
 * continued for sake of same-ness, tightly coupling view to persistence (as this design
 * does) circumvents the intent of MVC (no M nor decoupled C).  Compromising the design,
 * therefore, was not considered a significant problem.  Further, this design should not
 * be used as a model for future efforts.
 */
public class PWorker {
	  private static Logger logger = org.apache.logging.log4j.LogManager.getLogger(PWorker.class);
      void adminAuthorizationFormSelect(AdminAuthorizationForm parmForm, Connection parmConnection) throws SQLException {
      if (parmForm == null)
            return;
      ResultSet theResults = dbSelectAuthorizedUser(parmConnection);
      if (theResults == null)
            return;
        ArrayList theUsers = new ArrayList();
        while (theResults.next()) {
          String theUserId = theResults.getString(1);
          String theUserName = theResults.getString(2);
          AmFamUser toAdd = new AmFamUser(theUserId, theUserName);
          theUsers.add(toAdd);
        }
        parmForm.setExistingUser((AmFamUser[]) theUsers.toArray(new AmFamUser[theUsers.size()])) ;
        closeResultSet(theResults);
      }
final String MAIN_ENTRY_FORM_ADD_FULL = "mainentryformaddfull";
  final String MAIN_ENTRY_FORM_UPDATE = "mainentryformupdate";
  final String MAIN_ENTRY_FORM_SELECT = "mainentryformselect";
  Hashtable sqlHash = new Hashtable();
  /**
   * PWorker constructor.
   */
  public PWorker() {
    super();
  }
  /**
   * Deletes reporting indicator entries
   * for a given reporting key.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbDeleteReportingInd(long reportingkey, Connection connection) throws SQLException {
    logger.debug("dbDeleteReportingInd(long reportingkey, Connection connection)");
    Statement stmt = connection.createStatement();
    String qString = "";
    qString = "DELETE FROM LIFEREPL.LR_REPORTING_IND WHERE REPORTING_KEY = " + reportingkey + " ";
    logger.debug(qString);
    stmt.executeUpdate(qString);
    try {
      stmt.close();
    } catch (Exception ignore) {} //dump excpetion on close of stmt.
  }
  /**
   * Inserts new comment based on reportingkey
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param comment java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertComment(long reportingkey, String comment, String userid, Connection connection) throws SQLException {
	  logger.debug("dbInsertComment(long reportingkey, String comment, String userid, Connection connection)");
    String LR_Comment_Cols = "REPORTING_KEY, DATE_KEY, COMMENT_TEXT, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String testComment = wrap(comment);
    if (testComment.equals("null")) {
      testComment = "' '";
    }
    String qString =
      "INSERT INTO LIFEREPL.LR_COMMENT ("
        + LR_Comment_Cols
        + ") VALUES ( "
        + " '"
        + reportingkey
        + "', "
        + " "
        + getDateKey(connection)
        + ", "
        + " "
        + testComment
        + ", "
        + "'LRTSINCM', SYSDATE, '"
        + userid.toUpperCase()
        + "')";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no insert occurred
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Comment.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Inserts a company history entry.
   * Creation date: (2/11/2003 3:01:02 PM)
   * @param insuranceCoKey java.lang.String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertCompanyHistory(
    String insuranceCoKey,
    String companyName,
    String address1,
    String address2,
    String city,
    String state,
    String zip,
    String country,
    String showInList,
    String userid,
    Connection connection)
    throws SQLException {
	  logger.debug("dbInsertCompanyHistory(String companyName, String address1, String address2, String city, String state, String zip, String country, String userid, Connection connection)");
    String LR_Hist_Co_Cols =
      "INSURANCE_CO_KEY, HISTORY_ROW_TS, COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY_STATE, ";
    LR_Hist_Co_Cols += "COMPANY_ZIP, COMPANY_COUNTRY, COMPANY_SHOW_IN_LIST, HISTORY_ROW_USERID, HISTORY_ROW_PGMNAME";
    Statement stmt = connection.createStatement();
    String qString = "INSERT INTO LIFEREPL.LR_INSURANCE_COMPANY_HIST (" + LR_Hist_Co_Cols + ") ";
    qString += "VALUES ("
      + wrap(insuranceCoKey)
      + ", "
      + "SYSDATE, "
      + wrap(companyName)
      + ", "
      + wrap(address1)
      + ", "
      + wrap(address2)
      + ", "
      + wrap(city)
      + ", "
      + wrap(state)
      + ", "
      + wrap(zip)
      + ", "
      + wrap(country)
      + ", "
      + wrap(showInList)
      + ", "
      + wrap(userid)
      + ", "
      + "'LRTSCOHS')";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no inserts happened
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Company History.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Inserts a new company entry.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewCompany(
    String companyName,
    String address1,
    String address2,
    String city,
    String state,
    String zip,
    String country,
    String displayed,
    String userid,
    Connection connection)
    throws SQLException {
	  logger.debug("dbInsertNewCompany(String companyName, String address1, String address2, String city, String state, String zip, String country, String displayed, String userid, Connection connection)");
    String LR_Ins_Co_Cols = "COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY_STATE, ";
    LR_Ins_Co_Cols += "COMPANY_ZIP, COMPANY_COUNTRY, COMPANY_SHOW_IN_LIST, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Ins_Co_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String qString = "INSERT INTO LIFEREPL.LR_INSURANCE_COMPANY (" + LR_Ins_Co_Cols + ") ";
    qString += "VALUES ("
      + wrap(companyName)
      + ", "
      + wrap(address1)
      + ", "
      + wrap(address2)
      + ", "
      + wrap(city)
      + ", "
      + wrap(state)
      + ", "
      + wrap(zip)
      + ", "
      + wrap(country)
      + ", "
      + wrap(displayed)
      + ", "
      + "'LRTSINCO', SYSDATE, "
      + wrap(userid)
      + ", "
      + "null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Company.");
    }
    qString = "SELECT LIFEREPL.INSURANCE_CO_KEY_SEQ.CURRVAL FROM DUAL"; //get the next insurance co key from sequence
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      returnVal = rs.getString(1); // return insurance co key
    } else { // if sequence is broken....
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("Sequence Current Value not found for INSURANCE_CO_KEY_SEQ");
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Insert a new policy entry.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @return long
   * @param plancode java.lang.String
   * @param state java.lang.String
   * @param product java.lang.String
   * @param company java.lang.String
   * @param policynumber java.lang.String
   * @param lastname java.lang.String
   * @param firstname java.lang.String
   * @param middleinitial java.lang.String
   * @param suffix java.lang.String
   * @param agentcode java.lang.String
   * @param district java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbInsertPolicy(
    String plancode,
    String state,
    String product,
    String company,
    String policynumber,
    String lastname,
    String firstname,
    String middleinitial,
    String suffix,
    String agentcode,
    String district,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbInsertPolicy(String plancode, String state, String product, String company, String policynumber, String lastname, String firstname, String middleinitial, String suffix, String agentcode, String district, String userid, Connection connection)");
    String LR_Policy_Cols = "DATE_KEY, PLAN_CODE_KEY, CONTRACT_STATE_ID, PRODUCT_TYPE_KEY, INSURANCE_CO_KEY, ";
    LR_Policy_Cols += "POLICY_NUMBER, OWNER_LAST_NAME, OWNER_FIRST_NAME, OWNER_MI, OWNER_SUFFIX, OWNER_DOB, ";
    LR_Policy_Cols += "OWNER_SSN, INSURED_LAST_NAME, INSURED_FIRST_NAME, INSURED_MI, INSURED_SUFFIX, ";
    LR_Policy_Cols += "INSURED_DOB, INSURED_SSN, AGT_CD, DIST_CD, AGT_ID, SERVICING_AGT_CD, SERVICING_DIST_CD, ";
    LR_Policy_Cols += "SERVICING_AGT_ID, AMOUNT_OF_INS, POLICY_STATUS, REPLACEMENT_IND, POLICY_EFF_DATE, ";
    LR_Policy_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    //these "if"s added on 2/18/03 to comply with change to default certain fields.  Appears to be needed for reporting
    //side of system functionality....  C. Pauer - TeamSoft Inc. - 2/18/03
    if ((plancode == null) || (plancode.trim().equals(""))) {
      plancode = LrtsProperties.getDefaultPlanCode();
    }
    if ((agentcode == null) || (agentcode.trim().equals(""))) {
      agentcode = LrtsProperties.getDefaultAgent();
    }
    if ((district == null) || (district.trim().equals(""))) {
      district = LrtsProperties.getDefaultDistrict();
    }
    if ((state == null) || (state.trim().equals(""))) {
      state = LrtsProperties.getDefaultStateCode();
    }
    if ((product == null) || (product.trim().equals(""))) {
      product = LrtsProperties.getDefaultProductType();
    }
    ////////////////////////////////
    long returnVal = 0;
    String qString =
      "INSERT INTO LIFEREPL.LR_POLICY_GEN ("
        + LR_Policy_Cols
        + ") VALUES ("
        + getDateKey(connection)
        + ", "
        + wrap(plancode)
        + ", "
        + wrap(state)
        + ", "
        + wrap(product)
        + ", "
        + wrap(company)
        + ", "
        + wrap(policynumber)
        + ", "
        + wrap(lastname)
        + ", "
        + wrap(firstname)
        + ", "
        + wrap(middleinitial)
        + ", "
        + wrap(suffix)
        + ", "
        + "null, null, null, null, null, null, null, null, "
        + wrap(agentcode)
        + ", "
        + wrap(district)
        + ", null, "
        + "null, null, null, null, null, null, null, 'LRTSINPO', SYSDATE, '"
        + userid.toUpperCase()
        + "', null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no records were inserted
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Policy.");
    }
    qString = "SELECT LIFEREPL.POLICY_KEY_SEQ.CURRVAL FROM DUAL"; //get the sequence number that the insert generated.
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) { //if a sequence was read.
      returnVal = rs.getLong(1);
    } else { //sequence is broken...
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("Sequence Cuurent Value not found for POLICY_KEY_SEQ");
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Insert a new record in the Policy Actual table.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param replacingPolicyKey long
   * @param replacedPolicykey long
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertPolicyActual(long replacingPolicyKey, long replacedPolicykey, String userid, Connection connection)
    throws SQLException {
	logger.debug("dbInsertPolicyActual(long replacingPolicyKey, long replacedPolicykey, String userid, Connection connection)");
    String LR_POL_ACT_Cols =
      "REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, DATE_KEY, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String qString =
      "INSERT INTO LIFEREPL.LR_POLICY_ACTUAL ("
        + LR_POL_ACT_Cols
        + ") VALUES ("
        + "'"
        + replacingPolicyKey
        + "', '"
        + replacedPolicykey
        + "', "
        + getDateKey(connection)
        + ", 'LRTSINPA', SYSDATE, '"
        + userid.toUpperCase()
        + "', null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no records were inserted
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Policy Actual.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert a new policy history entry.
   * Creation date: (2/11/2003 3:04:54 PM)
   * @param policyKey java.lang.String
   * @param dateKey java.lang.String
   * @param plancode java.lang.String
   * @param state java.lang.String
   * @param product java.lang.String
   * @param company java.lang.String
   * @param policynumber java.lang.String
   * @param olastname java.lang.String
   * @param ofirstname java.lang.String
   * @param omiddleinitial java.lang.String
   * @param osuffix java.lang.String
   * @param odob java.lang.String
   * @param ossn java.lang.String
   * @param lastname java.lang.String
   * @param firstname java.lang.String
   * @param middleinitial java.lang.String
   * @param suffix java.lang.String
   * @param dob java.lang.String
   * @param ssn java.lang.String
   * @param agentcode java.lang.String
   * @param district java.lang.String
   * @param agendid java.lang.String
   * @param sagentcode java.lang.String
   * @param sdistrict java.lang.String
   * @param sagendid java.lang.String
   * @param amountOfIns java.lang.String
   * @param policyStatus java.lang.String
   * @param repInd java.lang.String
   * @param policyEffDate java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertPolicyHistory(
    String policyKey,
    String dateKey,
    String plancode,
    String state,
    String product,
    String company,
    String policynumber,
    String olastname,
    String ofirstname,
    String omiddleinitial,
    String osuffix,
    String odob,
    String ossn,
    String lastname,
    String firstname,
    String middleinitial,
    String suffix,
    String dob,
    String ssn,
    String agentcode,
    String district,
    String agentid,
    String sagentcode,
    String sdistrict,
    String sagentid,
    String amountOfIns,
    String policyStatus,
    String repInd,
    String policyEffDate,
    String userid,
    Connection connection)
    throws SQLException {
	  logger.debug("dbInsertPolicyHistory(String policyKey, String dateKey, String plancode, String state, String product, String company,"
        + "String policynumber, String olastname, String ofirstname, String omiddleinitial, String osuffix, String odob, String ossn, "
        + "String lastname, String firstname, String middleinitial, String suffix, String dob, String ssn, "
        + "String agentcode, String district, String agentid, String sagentcode, String sdistrict, String sagentid, "
        + "String amountOfIns, String policyStatus, String repInd, String policyEffDate, "
        + "String userid, Connection connection");
    String LR_Hist_Policy_Cols = "POLICY_KEY, DATE_KEY, HISTORY_ROW_TS, PLAN_CODE_KEY, CONTRACT_STATE_ID, ";
    LR_Hist_Policy_Cols += "PRODUCT_TYPE_KEY, INSURANCE_CO_KEY, ";
    LR_Hist_Policy_Cols += "POLICY_NUMBER, OWNER_LAST_NAME, OWNER_FIRST_NAME, OWNER_MI, OWNER_SUFFIX, OWNER_DOB, ";
    LR_Hist_Policy_Cols += "OWNER_SSN, INSURED_LAST_NAME, INSURED_FIRST_NAME, INSURED_MI, INSURED_SUFFIX, ";
    LR_Hist_Policy_Cols += "INSURED_DOB, INSURED_SSN, AGT_CD, DIST_CD, AGT_ID, SERVICING_AGT_CD, SERVICING_DIST_CD, ";
    LR_Hist_Policy_Cols += "SERVICING_AGT_ID, AMOUNT_OF_INS, POLICY_STATUS, REPLACEMENT_IND, POLICY_EFF_DATE, ";
    LR_Hist_Policy_Cols += "HISTORY_ROW_PGMNAME, HISTORY_ROW_USERID";
    Statement stmt = connection.createStatement();
    String qString =
      "INSERT INTO LIFEREPL.LR_POLICY_GEN_HIST ("
        + LR_Hist_Policy_Cols
        + ") VALUES ("
        + wrap(policyKey)
        + ", "
        + wrap(dateKey)
        + ", "
        + "SYSDATE, "
        + wrap(plancode)
        + ", "
        + wrap(state)
        + ", "
        + wrap(product)
        + ", "
        + wrap(company)
        + ", "
        + wrap(policynumber)
        + ", "
        + wrap(olastname)
        + ", "
        + wrap(ofirstname)
        + ", "
        + wrap(omiddleinitial)
        + ", "
        + wrap(osuffix)
        + ", "
        + wrap(odob)
        + ", "
        + wrap(ossn)
        + ", "
        + wrap(lastname)
        + ", "
        + wrap(firstname)
        + ", "
        + wrap(middleinitial)
        + ", "
        + wrap(suffix)
        + ", "
        + wrap(dob)
        + ", "
        + wrap(ssn)
        + ", "
        + wrap(agentcode)
        + ", "
        + wrap(district)
        + ", "
        + wrap(agentid)
        + ", "
        + wrap(sagentcode)
        + ", "
        + wrap(sdistrict)
        + ", "
        + wrap(sagentid)
        + ", "
        + wrap(amountOfIns)
        + ", "
        + wrap(policyStatus)
        + ", "
        + wrap(repInd)
        + ", "
        + wrap(policyEffDate)
        + ", "
        + "'LRTSPOHS', '"
        + userid.toUpperCase()
        + "')";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no records were inserted
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Policy History.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert an entry into policy report table.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param replacingPolicyKey long
   * @param replacedPolicyKey long
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertPolicyReport(
    long reportingkey,
    long replacingPolicyKey,
    long replacedPolicyKey,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbInsertPolicyReport(long reportingkey, long replacingPolicyKey, long replacedPolicykey, String userid, Connection connection)");
    String LR_POL_REP_Cols =
      "REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, DATE_KEY, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String qString =
      "INSERT INTO LIFEREPL.LR_POLICY_REPORT ("
        + LR_POL_REP_Cols
        + ") VALUES ("
        + "'"
        + reportingkey
        + "', '"
        + replacingPolicyKey
        + "', '"
        + replacedPolicyKey
        + "', "
        + getDateKey(connection)
        + ", 'LRTSINPR', SYSDATE, '"
        + userid.toUpperCase()
        + "', null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no record was inserted...
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Policy Report.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert a new entry in the Reporting table
   * Creation date: (1/2/2003 3:08:36 PM)
   * @return long
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbInsertReport(String userid, Connection connection) throws SQLException {
    logger.debug("dbInsertReport(String userid, Connection connection)");
    String LR_Reporting_Cols =
      "DATE_KEY, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    long returnVal = 0;
    String qString =
      "INSERT INTO LIFEREPL.LR_REPORTING ("
        + LR_Reporting_Cols
        + ") VALUES ("
        + getDateKey(connection)
        + ", 'LRTSINRP', SYSDATE, '"
        + userid
        + "', null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no record inserted...
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Report.");
    }
    qString = "SELECT LIFEREPL.REPORTING_KEY_SEQ.CURRVAL FROM DUAL";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) { //get sequence number that was generated....
      returnVal = rs.getLong(1);
    } else { //if sequence is broken...
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("Sequence Cuurent Value not found for REPORTING_KEY_SEQ");
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Insert appropriate entries in the reporting indicator
   * table.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param comment java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertReportingInd(long reportingkey, MainEntryForm mef, Connection connection) throws SQLException {
	logger.debug("dbInsertReportingInd(long reportingkey, MainEntryForm mef, Connection connection)");
    //this method is more brute force than the other dbXXXXX routines, in that the specific values need to be checked on the
    //form, to see if they are there, and then we have to lookup the key that matches the string for that form setting on the
    //indicator table.  I would describe this as somewhat of a disconnect between the screen presentation and the database
    //structure: the data is presented as discreet values with very different purposes, but on the database, the "indicators", a
    //somewhat arbitrary designation, are treated as members of a collection or list.
    //
    //One other unique behavior here is that the strings that have to be found in the ind_cd column on the indicator table are fetched
    //from a property file.  This is in case someone fudges with the strings in ind_cd.  If someone changes these strings, the app
    //will break.  Having them in the props file at least will allow a fix without code changing.
    String LR_REP_IND_Cols = "IND_KEY, REPORTING_KEY, DATE_KEY, IND_VALUE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    ResultSet rs = null;
    String qString = "";
    //each of these 3 if structures checks one indicator on the form....
    // IF 1  check disclosure and insert if necessary
    if ((mef.getDisclosed() != null) && !(mef.getDisclosed().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE "
          + " OR END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getDisclosureIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString); //reads the key on INDICATOR that matches the string for disclosure
      if (rs.next()) {
        qString =
          "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
            + LR_REP_IND_Cols
            + ") VALUES ("
            + "'"
            + rs.getLong(1)
            + "', '"
            + reportingkey
            + "', "
            + getDateKey(connection)
            + ",  '"
            + mef.getDisclosed()
            + "', 'LRTSINRI', SYSDATE, '"
            + mef.getUserId()
            + "')";
        //if the string matches, we have the key and can update the reporting_ind table
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { //if no record was inserted....
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No insert performed on reporting indicators.");
        }
      }
    }
    //END OF IF 1
    //IF 2  check if delete from report was indicated
    if ((mef.getDeleteFromReport() != null) && !(mef.getDeleteFromReport().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE  OR "
          + " END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getReportingIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString); //reads the key on INDICATOR that matches the string for reporting indicator
      String reportString = "";
      //This next if is done, because the screen prompt reads "Delete from Report?"...but the value on the database
      //represents reporting indicator, or in english, should we report on this set of records?
      //So the value has to flipped from Y to N and vice versa to accurately reflect what the table
      //column represents.
      if (mef.getDeleteFromReport().trim().equalsIgnoreCase("Y")) {
        reportString = "N";
      } else {
        reportString = "Y";
      }
      if (rs.next()) {
        qString =
          "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
            + LR_REP_IND_Cols
            + ") VALUES ("
            + "'"
            + rs.getLong(1)
            + "', '"
            + reportingkey
            + "', "
            + getDateKey(connection)
            + ", '"
            + reportString
            + "', 'LRTSINRI', SYSDATE, '"
            + mef.getUserId()
            + "')";
        //if the string matches, we have the key and can update the reporting_ind table
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { //if no record was inserted....
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No insert performed on report indicator.");
        }
      }
    }
    //END OF IF 2
    //IF 3  check if replacement type was filled in...
    if ((mef.getReplacementType() != null) && !(mef.getReplacementType().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE "
          + " OR END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getReplacementIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString); //reads the key on INDICATOR that matches the string for replacement type
      if (rs.next()) {
        qString =
          "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
            + LR_REP_IND_Cols
            + ") VALUES ("
            + "'"
            + rs.getLong(1)
            + "', '"
            + reportingkey
            + "', "
            + getDateKey(connection)
            + ",  '"
            + mef.getReplacementType()
            + "', 'LRTSINRI', SYSDATE, '"
            + mef.getUserId()
            + "')";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { //if no record was inserted....
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No insert performed on report indicators.");
        }
      }
    }
    //END OF IF 3
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert a reporting indicator history entry.
   * Creation date: (1/21/2003 9:42:55 AM)
   * @param indKey java.lang.String
   * @param  reportingKey java.lang.String
   * @param  dateKey java.lang.String
   * @param  indValue java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertReportingIndHistory(
    String indKey,
    String reportingKey,
    String dateKey,
    String indValue,
    String userid,
    Connection connection)
    throws SQLException {
    //database changes incorporated
	logger.debug("dbInsertTransactionHistory(String indKey, String reportingKey, String dateKey, String indValue, String userid, Connection connection)");
    String LR_Reporting_Ind_Hist_Cols =
      "IND_KEY, REPORTING_KEY, HISTORY_ROW_TS, DATE_KEY, IND_VALUE, HISTORY_ROW_PGMNAME, HISTORY_ROW_USERID";
    Statement stmt = connection.createStatement();
    String qString =
      "INSERT INTO LIFEREPL.LR_REPORTING_IND_HIST ("
        + LR_Reporting_Ind_Hist_Cols
        + ") VALUES ("
        + wrap(indKey)
        + ", "
        + wrap(reportingKey)
        + ", "
        + " SYSDATE, "
        + wrap(dateKey)
        + ", "
        + wrap(indValue)
        + ", "
        + "'LRTSINIH', "
        + wrap(userid)
        + ")";
    logger.debug(qString);
    stmt.executeUpdate(qString);
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert new Transaction Entry
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param replacingkey long
   * @param replacedkey long
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbInsertTransaction(
    long reportingkey,
    long replacingPolicyKey,
    long replacedPolicykey,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbInsertTransaction(long reportingkey, long replacingPolicyKey, long replacedPolicykey, String userid, Connection connection)");
    String LR_Transaction_Cols =
      " REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, DATE_KEY, TRX_CODE, PROCESS_DATE, TRANS_DATE, TRANS_AMOUNT, CASH_VALUE, "
        + " PAID_TO_DATE, COMPLETE_DATE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
      String qString =
        "INSERT INTO LIFEREPL.LR_TRANSACTION ("
          + LR_Transaction_Cols
          + ") VALUES ( "
          + "'"
          + reportingkey
          + "', "
          + "'"
          + replacingPolicyKey
          + "', "
          + "'"
          + replacedPolicykey
          + "', "
          + " "
          + getDateKey(connection)
          + ", "
          + "'TB', "
    + //business rule: new transaction written by this system is always "TB" TRX_CODE
  "null, "
    + "null, "
    + "null, "
    + "null, "
    + "null, "
    + "null, 'LRTSINTR', SYSDATE, '"
    + userid.toUpperCase()
    + "', null, null, null)";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no records were inserted
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Transaction.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Insert transaction history entry.
   * Creation date: (2/11/2003 9:42:55 AM)
   * @param transactionkey java.lang.String
   * @param dateKey java.lang.String
   * @param reportingkey java.lang.String
   * @param replacingPolicyKey java.lang.String
   * @param replacedPolicykey java.lang.String
   * @param trxCode java.lang.String
   * @param processDate java.lang.String
   * @param transDate java.lang.String
   * @param transAmt java.lang.String
   * @param cashVal java.lang.String
   * @param paidToDate java.lang.String
   * @param completeDate java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   **/
  private void dbInsertTransactionHistory(
    String transactionkey,
    String dateKey,
    String reportingkey,
    String replacingPolicyKey,
    String replacedPolicykey,
    String trxCode,
    String processDate,
    String transDate,
    String transAmt,
    String cashVal,
    String paidToDate,
    String completeDate,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbInsertTransactionHistory(String transactionkey, String dateKey, String reportingkey, "
        + "String replacingPolicyKey, String replacedPolicykey, String trxCode, String processDate, "
        + "String transDate, String cashVal, String paidToDate, String completeDate, String userid, Connection connection");
    String LR_Transaction_Hist_Cols =
      "TRANSACTION_KEY, HISTORY_ROW_TS, DATE_KEY,  REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, "
        + "TRX_CODE, PROCESS_DATE, TRANS_DATE, TRANS_AMOUNT, CASH_VALUE, PAID_TO_DATE, COMPLETE_DATE, HISTORY_ROW_PGMNAME, HISTORY_ROW_USERID";
    Statement stmt = connection.createStatement();
    if ((paidToDate != null) && (!(paidToDate.trim().equals("")))) {
      if (paidToDate.length() > 10) {
        paidToDate = paidToDate.substring(0, 10);
      }
    }
    if ((transDate != null) && (!(transDate.trim().equals("")))) {
      if (transDate.length() > 10) {
        transDate = transDate.substring(0, 10);
      }
    }
    if ((processDate != null) && (!(processDate.trim().equals("")))) {
      if (processDate.length() > 10) {
        processDate = processDate.substring(0, 10);
      }
    }
    if ((completeDate != null) && (!(completeDate.trim().equals("")))) {
      try {
        if (completeDate.length() > 10) {
          completeDate = completeDate.substring(0, 10);
        }
        completeDate = LrtsDateHandler.convertDateToSaveString(LrtsDateHandler.convertSaveStringToDate(completeDate));
      } catch (ParseException pe) {}
    }
    String qString =
      "INSERT INTO LIFEREPL.LR_TRANSACTION_HIST ("
        + LR_Transaction_Hist_Cols
        + ") VALUES ("
        + "'"
        + transactionkey
        + "', "
        + " SYSDATE, "
        + wrap(dateKey)
        + ", "
        + wrap(reportingkey)
        + ", "
        + wrap(replacingPolicyKey)
        + ", "
        + wrap(replacedPolicykey)
        + ", "
        + wrap(trxCode)
        + ", "
        + wrap(processDate)
        + ", "
        + wrap(transDate)
        + ", "
        + wrap(transAmt)
        + ", "
        + wrap(cashVal)
        + ", "
        + wrap(paidToDate)
        + ", "
        + wrap(completeDate)
        + ", "
        + "'LRTSINTH', "
        + wrap(userid)
        + ")";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) { //if no records were inserted
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on Transaction History.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Select authorization level based on deptartment name.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return int
   * @param deptName java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private int dbSelectAuthorization(String deptName, Connection connection) throws SQLException {
    logger.debug("dbSelectAuthorization(String deptName, Connection connection)");
    //	String LR_Auth_Cols = "DEPARTMENT_NAME, AUTHORIZATION_LEVEL, EFF_DATE, END_EFF_DATE, CREATE_ROW_TS, ";
    //	LR_Auth_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_USERID, UPDATE_ROW_TS, UPDATE_ROW_USERID, UPDATE_ROW_PGMNAME";
    String qString = "";
    Statement stmt = null;
    ResultSet rs = null;
    int returnVal = -1; //routine will return -1 if department not found.
    stmt = connection.createStatement();
    qString =
      "SELECT AUTHORIZATION_LEVEL, EFF_DATE, END_EFF_DATE FROM LIFEREPL.LR_AUTHORIZATION WHERE "
        + " DEPARTMENT_NAME = '"
        + deptName
        + "' AND SYSDATE > EFF_DATE AND (END_EFF_DATE IS NULL OR SYSDATE < END_EFF_DATE)";
    logger.debug(qString);
    rs = stmt.executeQuery(qString);
    if (rs.next()) {
      returnVal = new Integer(rs.getString("AUTHORIZATION_LEVEL")).intValue();
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Select comment for this reporting key.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @return String
   * @param reportingkey long
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbSelectComment(long reportingkey, Connection connection) throws SQLException {
    logger.debug("dbSelectComment(long reportingkey, Connection connection)");
    //	String LR_Comment_Cols = "REPORTING_KEY, DATE_KEY, COMMENT_TEXT, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID";
    String returnVal = "";
    Statement stmt = connection.createStatement();
    String qString =
      "SELECT COMMENT_TEXT FROM LIFEREPL.LR_COMMENT WHERE REPORTING_KEY = " + reportingkey + " ORDER BY CREATE_ROW_TS DESC";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) { //if record found
      if (rs.getString("COMMENT_TEXT") != null) {
        returnVal = rs.getString("COMMENT_TEXT");
      } else {
        returnVal = "";
      }
    } else {
      returnVal = "";
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Selects the replaced and replacing company information
   * and adds it to the MainEntryForm
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param mef com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbSelectCompanies(MainEntryForm mef, Connection connection) throws SQLException {
    logger.debug("dbSelectCompanies(MainEntryForm mef, Connection connection)");
    //	String LR_Ins_Co_Cols = "COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY STATE, ";
    //	LR_Ins_Co_Cols += "COMPANY_ZIP, COMPANY_COUNTRY, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    //	LR_Ins_Co_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    ResultSet rs = null;
    //first handle replacing company.
    //check if it is aflic.....
    if (mef.getReplacingAflic().equalsIgnoreCase("Y")) {
      String qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + LrtsProperties.getAflicNumber() + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      // set the company entries in the MainEntryForm to the correct values...
      mef.setReplacingCompanyLookup(LrtsConstants.AMFAM_LOOKUP);
      if (rs.next()) {
        if (rs.getString("COMPANY_NAME") != null) {
          mef.setReplacingCompanyName(rs.getString("COMPANY_NAME"));
        }
        if (rs.getString("COMPANY_ADDRESS_LINE_1") != null) {
          mef.setReplacingAddress1(rs.getString("COMPANY_ADDRESS_LINE_1"));
        }
        if (rs.getString("COMPANY_ADDRESS_LINE_2") != null) {
          mef.setReplacingAddress2(rs.getString("COMPANY_ADDRESS_LINE_2"));
        }
        if (rs.getString("COMPANY_CITY") != null) {
          mef.setReplacingCity(rs.getString("COMPANY_CITY"));
        }
        if (rs.getString("COMPANY_STATE") != null) {
          mef.setReplacingState(rs.getString("COMPANY_STATE"));
        }
        if (rs.getString("COMPANY_COUNTRY") != null) {
          mef.setReplacingCountry(rs.getString("COMPANY_COUNTRY"));
        }
        if (rs.getString("COMPANY_ZIP") != null) {
          mef.setReplacingZipCode(rs.getString("COMPANY_ZIP"));
        }
      }
    } else { // if not aflic
      // if either the lookup or key entry is populated, we can figure out what company this is.
      if ((mef.getReplacingCompanyLookup() != null && !(mef.getReplacingCompanyLookup().trim().equals("")))
        || (mef.getReplacingInsuranceCoKey() != null && !(mef.getReplacingInsuranceCoKey().trim().equals("")))) {
        String qString = "";
        if (mef.getReplacingCompanyLookup() != null && !(mef.getReplacingCompanyLookup().trim().equals(""))) {
          qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + mef.getReplacingCompanyLookup() + "' ";
        } else {
          qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + mef.getReplacingInsuranceCoKey() + "' ";
        }
        logger.debug(qString);
        rs = stmt.executeQuery(qString);
        if (rs.next()) {
          if (rs.getString("COMPANY_NAME") != null) {
            mef.setReplacingCompanyName(rs.getString("COMPANY_NAME"));
          }
          //The company entries fall into two categories: if "show in list" is yes, then the
          //company is well documented, should be shown in the drop down on the presentation screens
          //If not, then it was entered as a new company on one of the entries for this system.  That
          //record, even if it could document a later company entry is not used again because of the possibilty
          //of inaccurate data.  It is only associated with this policy number entry on  policy_gen.
          if (rs.getString("COMPANY_SHOW_IN_LIST").equalsIgnoreCase("N")) {
            mef.setReplacingCompanyLookup(" ");
          } else {
            if ((mef.getReplacingCompanyLookup() == null) || (mef.getReplacingCompanyLookup().trim().equals(""))) {
              mef.setReplacingCompanyLookup(mef.getReplacingInsuranceCoKey());
            }
          }
          if (rs.getString("COMPANY_ADDRESS_LINE_1") != null) {
            mef.setReplacingAddress1(rs.getString("COMPANY_ADDRESS_LINE_1"));
          }
          if (rs.getString("COMPANY_ADDRESS_LINE_2") != null) {
            mef.setReplacingAddress2(rs.getString("COMPANY_ADDRESS_LINE_2"));
          }
          if (rs.getString("COMPANY_CITY") != null) {
            mef.setReplacingCity(rs.getString("COMPANY_CITY"));
          }
          if (rs.getString("COMPANY_STATE") != null) {
            mef.setReplacingState(rs.getString("COMPANY_STATE"));
          }
          if (rs.getString("COMPANY_COUNTRY") != null) {
            mef.setReplacingCountry(rs.getString("COMPANY_COUNTRY"));
          }
          if (rs.getString("COMPANY_ZIP") != null) {
            mef.setReplacingZipCode(rs.getString("COMPANY_ZIP"));
          }
        }
      }
    }
    // same as if structure above except for replaced entries....
    if (mef.getReplacedAflic().equalsIgnoreCase("Y")) {
      String qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + LrtsConstants.AMFAM_LOOKUP + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      mef.setReplacedCompanyLookup(LrtsConstants.AMFAM_LOOKUP);
      if (rs.next()) {
        if (rs.getString("COMPANY_NAME") != null) {
          mef.setReplacedCompanyName(rs.getString("COMPANY_NAME"));
        }
        if (rs.getString("COMPANY_ADDRESS_LINE_1") != null) {
          mef.setReplacedAddress1(rs.getString("COMPANY_ADDRESS_LINE_1"));
        }
        if (rs.getString("COMPANY_ADDRESS_LINE_2") != null) {
          mef.setReplacedAddress2(rs.getString("COMPANY_ADDRESS_LINE_2"));
        }
        if (rs.getString("COMPANY_CITY") != null) {
          mef.setReplacedCity(rs.getString("COMPANY_CITY"));
        }
        if (rs.getString("COMPANY_STATE") != null) {
          mef.setReplacedState(rs.getString("COMPANY_STATE"));
        }
        if (rs.getString("COMPANY_COUNTRY") != null) {
          mef.setReplacedCountry(rs.getString("COMPANY_COUNTRY"));
        }
        if (rs.getString("COMPANY_ZIP") != null) {
          mef.setReplacedZipCode(rs.getString("COMPANY_ZIP"));
        }
      }
    } else {
      if ((mef.getReplacedCompanyLookup() != null && !(mef.getReplacedCompanyLookup().trim().equals("")))
        || (mef.getReplacedInsuranceCoKey() != null && !(mef.getReplacedInsuranceCoKey().trim().equals("")))) {
        String qString = "";
        if (mef.getReplacedCompanyLookup() != null && !(mef.getReplacedCompanyLookup().trim().equals(""))) {
          qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + mef.getReplacedCompanyLookup() + "' ";
        } else {
          qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + mef.getReplacedInsuranceCoKey() + "' ";
        }
        logger.debug(qString);
        rs = stmt.executeQuery(qString);
        if (rs.next()) {
          if (rs.getString("COMPANY_NAME") != null) {
            mef.setReplacedCompanyName(rs.getString("COMPANY_NAME"));
          }
          if (rs.getString("COMPANY_ADDRESS_LINE_1") != null) {
            mef.setReplacedAddress1(rs.getString("COMPANY_ADDRESS_LINE_1"));
          }
          if (rs.getString("COMPANY_SHOW_IN_LIST").equalsIgnoreCase("N")) {
            mef.setReplacedCompanyLookup(" ");
          } else {
            if ((mef.getReplacedCompanyLookup() == null) || (mef.getReplacedCompanyLookup().trim().equals(""))) {
              mef.setReplacedCompanyLookup(mef.getReplacedInsuranceCoKey());
            }
          }
          if (rs.getString("COMPANY_ADDRESS_LINE_2") != null) {
            mef.setReplacedAddress2(rs.getString("COMPANY_ADDRESS_LINE_2"));
          }
          if (rs.getString("COMPANY_CITY") != null) {
            mef.setReplacedCity(rs.getString("COMPANY_CITY"));
          }
          if (rs.getString("COMPANY_STATE") != null) {
            mef.setReplacedState(rs.getString("COMPANY_STATE"));
          }
          if (rs.getString("COMPANY_COUNTRY") != null) {
            mef.setReplacedCountry(rs.getString("COMPANY_COUNTRY"));
          }
          if (rs.getString("COMPANY_ZIP") != null) {
            mef.setReplacedZipCode(rs.getString("COMPANY_ZIP"));
          }
        }
      }
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Select the correct policy entries from Policy_Gen table.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param mef com.amfam.lrts.form.MainEntryForm
   * @param replacingPolicyKey long
   * @param replacedPolicyKey long
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbSelectPolicies(MainEntryForm mef, long replacingPolicyKey, long replacedPolicyKey, Connection connection)
    throws SQLException {
	logger.debug("dbSelectPolicies(MainEntryForm mef, long replacingPolicyKey, long replacedPolicyKey,  Connection connection)");
    //	String LR_Policy_Cols = "DATE_KEY, PLAN_CODE_KEY, CONTRACT_STATE_ID, PRODUCT_TYPE_KEY, INSURANCE_CO_KEY, ";
    //	LR_Policy_Cols += "POLICY_NUMBER, OWNER_LAST_NAME, OWNER_FIRST_NAME, OWNER_MI, OWNER_SUFFIX, OWNER_DOB, ";
    //	LR_Policy_Cols += "OWNER_SSN, INSURED_LAST_NAME, INSURED_FIRST_NAME, INSURED_MI, INSURED_SUFFIX, ";
    //	LR_Policy_Cols += "INSURED_DOB, INSURED_SSN, AGT_CD, DIST_CD, AGT_ID, SERVICING_AGT_CD, SERVICING_DIST_CD, ";
    //	LR_Policy_Cols += "SERVICING_AGT_ID, AMOUNT_OF_INS, POLICY_STATUS, REPLACEMENT_IND, POLICY_EFF_DATE, ";
    //	LR_Policy_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    String qString = "";
    Statement stmt = null;
    ResultSet rs = null;
    //the policy key is set to -1 at a different spot: the non-aflic companies are not required to enter a policy number and
    //so this entry in the policy_gen might be selectable on that basis.
    if (replacingPolicyKey != -1) {
      stmt = connection.createStatement();
      qString = "SELECT * FROM LIFEREPL.LR_POLICY_GEN WHERE POLICY_KEY = '" + replacingPolicyKey + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        if ((rs.getString("PLAN_CODE_KEY") != null) && (!(rs.getString("PLAN_CODE_KEY").equals(LrtsProperties.getDefaultPlanCode())))) {
          mef.setReplacingPlanCode(rs.getString("PLAN_CODE_KEY"));
        }
        if ((rs.getString("CONTRACT_STATE_ID") != null)
          && (!(rs.getString("CONTRACT_STATE_ID").equals(LrtsProperties.getDefaultStateCode())))) {
          mef.setReplacingContractState(rs.getString("CONTRACT_STATE_ID"));
        }
        if ((rs.getString("PRODUCT_TYPE_KEY") != null)
          && (!(rs.getString("PRODUCT_TYPE_KEY").equals(LrtsProperties.getDefaultProductType())))) {
          mef.setReplacingProduct(rs.getString("PRODUCT_TYPE_KEY"));
        }
        if (rs.getString("INSURANCE_CO_KEY") != null) {
          mef.setReplacingInsuranceCoKey(rs.getString("INSURANCE_CO_KEY"));
          if (mef.getReplacingInsuranceCoKey().equals(Integer.toString(LrtsProperties.getAflicNumber()))) {
            mef.setReplacingAflic(LrtsConstants.YES); //if it's aflic, set the correct indicator
          }
        }
        if (rs.getString("POLICY_NUMBER") != null) {
          mef.setReplacingPolicyNumber(rs.getString("POLICY_NUMBER"));
        }
        if (rs.getString("OWNER_LAST_NAME") != null) {
          mef.setReplacingLastName(rs.getString("OWNER_LAST_NAME"));
        }
        if (rs.getString("OWNER_FIRST_NAME") != null) {
          mef.setReplacingFirstName(rs.getString("OWNER_FIRST_NAME"));
        }
        if (rs.getString("OWNER_MI") != null) {
          mef.setReplacingMiddleInitial(rs.getString("OWNER_MI"));
        }
        if (rs.getString("OWNER_SUFFIX") != null) {
          mef.setReplacingSuffix(rs.getString("OWNER_SUFFIX"));
        }
        if ((rs.getString("AGT_CD") != null) && (!(rs.getString("AGT_CD").equals(LrtsProperties.getDefaultAgent())))) {
          mef.setReplacingAgentCode(rs.getString("AGT_CD"));
        }
        if ((rs.getString("DIST_CD") != null) && (!(rs.getString("DIST_CD").equals(LrtsProperties.getDefaultDistrict())))) {
          mef.setReplacingDistrictCode(rs.getString("DIST_CD"));
        }
      }
    }
    if (replacedPolicyKey != -1) {
      qString = "SELECT * FROM LIFEREPL.LR_POLICY_GEN WHERE POLICY_KEY = '" + replacedPolicyKey + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        if ((rs.getString("PLAN_CODE_KEY") != null) && (!(rs.getString("PLAN_CODE_KEY").equals(LrtsProperties.getDefaultPlanCode())))) {
          mef.setReplacedPlanCode(rs.getString("PLAN_CODE_KEY"));
        }
        if ((rs.getString("CONTRACT_STATE_ID") != null)
          && (!(rs.getString("CONTRACT_STATE_ID").equals(LrtsProperties.getDefaultStateCode())))) {
          mef.setReplacedContractState(rs.getString("CONTRACT_STATE_ID"));
        }
        if ((rs.getString("PRODUCT_TYPE_KEY") != null)
          && (!(rs.getString("PRODUCT_TYPE_KEY").equals(LrtsProperties.getDefaultProductType())))) {
          mef.setReplacedProduct(rs.getString("PRODUCT_TYPE_KEY"));
        }
        if (rs.getString("INSURANCE_CO_KEY") != null) {
          mef.setReplacedInsuranceCoKey(rs.getString("INSURANCE_CO_KEY"));
          if (mef.getReplacedInsuranceCoKey().equals(Integer.toString(LrtsProperties.getAflicNumber()))) {
            mef.setReplacedAflic(LrtsConstants.YES); //if it's aflic, set the correct indicator
          }
        }
        if (rs.getString("POLICY_NUMBER") != null) {
          mef.setReplacedPolicyNumber(rs.getString("POLICY_NUMBER"));
        }
        if (rs.getString("OWNER_LAST_NAME") != null) {
          mef.setReplacedLastName(rs.getString("OWNER_LAST_NAME"));
        }
        if (rs.getString("OWNER_FIRST_NAME") != null) {
          mef.setReplacedFirstName(rs.getString("OWNER_FIRST_NAME"));
        }
        if (rs.getString("OWNER_MI") != null) {
          mef.setReplacedMiddleInitial(rs.getString("OWNER_MI"));
        }
        if (rs.getString("OWNER_SUFFIX") != null) {
          mef.setReplacedSuffix(rs.getString("OWNER_SUFFIX"));
        }
        if ((rs.getString("AGT_CD") != null) && (!(rs.getString("AGT_CD").equals(LrtsProperties.getDefaultAgent())))) {
          mef.setReplacedAgentCode(rs.getString("AGT_CD"));
        }
        if ((rs.getString("DIST_CD") != null) && (!(rs.getString("DIST_CD").equals(LrtsProperties.getDefaultDistrict())))) {
          mef.setReplacedDistrictCode(rs.getString("DIST_CD"));
        }
      }
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Checks for an existing policy key and if it does
   * not find one, returns -1.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @return long
   * @param policynumber java.lang.String
   * @param companyKey java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbSelectPolicyKey(String policyNumber, String companyKey, Connection connection) throws SQLException {
	logger.debug("dbSelectPolicyKey(String policyNumber, String companyKey, Connection connection)");
    Statement stmt = connection.createStatement();
    long returnVal = -1;
    String qString =
      "SELECT POLICY_KEY FROM LIFEREPL.LR_POLICY_GEN WHERE "
        + " POLICY_NUMBER = '"
        + policyNumber
        + "' AND INSURANCE_CO_KEY = '"
        + companyKey
        + "'  ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      returnVal = rs.getLong(1);
    } else {
      returnVal = -1; // this is a normal case, non-aflic policies are not required to enter a policy number.
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Select reporting key based on the policy keys.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param replacingPolicyKey long
   * @param replacedPolicyKey long
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbSelectPolicyReport(long replacingPolicyKey, long replacedPolicyKey, Connection connection) throws SQLException {
	logger.debug("dbSelectPolicyReport(long replacingPolicyKey, long replacedPolicykey, Connection connection)");
    //	String LR_POL_REP_Cols = "REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, DATE_KEY, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    long returnVal = -1;
    Statement stmt = connection.createStatement();
    String qString =
      "SELECT REPORTING_KEY FROM LIFEREPL.LR_POLICY_REPORT WHERE "
        + "REPLACING_POLICY_KEY = '"
        + replacingPolicyKey
        + "' AND REPLACED_POLICY_KEY = '"
        + replacedPolicyKey
        + "'";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      returnVal = rs.getLong(1);
    } else {
      returnVal = 0L;
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }

//================================ plan code & prod type
 /**
   * Selects the company information and adds it to the PlanCodeUpdateForm
   * @param icu com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  private void dbSelectPlanCode(PlanCodeUpdateForm pcu, Connection connection) throws SQLException {
	logger.debug("dbSelectCompanies(PlanCodeUpdateForm icu, Connection connection)");

    Statement stmt = connection.createStatement();
    ResultSet rs = null;

    String yesNoInd= null;

    // Build the SELECT statement.
    String qString = "SELECT * FROM LIFEREPL.LR_PLAN_CD_GEN WHERE PLAN_CODE_KEY = '" + pcu.getRecordKey() + "' ";
    // Log the SELECT statement.
    logger.debug(qString);
    // Execute the SELECT statement.
    rs = stmt.executeQuery(qString);
    if (rs.next()) {




      if (rs.getString("PLAN_CODE") != null) {
        pcu.setPlanCode(rs.getString("PLAN_CODE"));
      }


      if (rs.getString("PRODUCT_TYPE_KEY") != null) {
        pcu.setProductTypeKey(rs.getString("PRODUCT_TYPE_KEY"));
//        System.out.println("from pworker: " + rs.getString("PRODUCT_TYPE_KEY"));
        //pcu.setNewProdType(null);
        //pcu.setCheckMe(null);
      }
      if (rs.getString("EFF_DATE") != null) {
        pcu.setStartDateUnformatted(rs.getString("EFF_DATE"));
        pcu.setStartDate(LrtsDateHandler.convertDateToOutputString(rs.getTimestamp("EFF_DATE")));
      }
      if (rs.getString("END_EFF_DATE") != null) {
        pcu.setEndDateUnformatted(rs.getString("END_EFF_DATE"));
        pcu.setEndDate(LrtsDateHandler.convertDateToOutputString(rs.getTimestamp("END_EFF_DATE")));
      }
      if (rs.getString("AFLIC_CURRENT_IND") != null) {
      	pcu.setDisplayed(rs.getString("AFLIC_CURRENT_IND"));
      }
    }

    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }



 /**
   * Selects the company information and adds it to the PlanCodeUpdateForm
   * @param icu com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  private void dbSelectProductType(ProductTypeUpdateForm pcu, Connection connection) throws SQLException {
	logger.debug("dbSelectCompanies(PlanCodeUpdateForm icu, Connection connection)");

    Statement stmt = connection.createStatement();
    ResultSet rs = null;
    // Build the SELECT statement.
    String qString = "SELECT * FROM LIFEREPL.LR_PRODUCT_TYPE WHERE PRODUCT_TYPE_KEY = '" + pcu.getSearchField() + "' ";
    // Log the SELECT statement.
    logger.debug(qString);
    // Execute the SELECT statement.
    rs = stmt.executeQuery(qString);
    if (rs.next()) {

     if (rs.getString("PRODUCT_TYPE_KEY") != null) {

     	pcu.setProductTypeKey(rs.getString("PRODUCT_TYPE_KEY"));
     	//System.out.println("key: " + rs.getString("PRODUCT_TYPE_KEY"));
     }

      if (rs.getString("PRODUCT_TYPE") != null) {
        pcu.setProductType(rs.getString("PRODUCT_TYPE"));
             	//System.out.println("type: " + rs.getString("PRODUCT_TYPE"));
      }

      if (rs.getString("AFLIC_CURRENT_IND") != null) {
        pcu.setCurrInd(rs.getString("AFLIC_CURRENT_IND"));
             	//System.out.println("ind: " + rs.getString("AFLIC_CURRENT_IND"));
      }

    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
//================================ end plan code


  /**
   * Select reporting indicators, based on the
   * reporting key, and store them in the
   * MainEntryForm
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param mef com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbSelectReportingInd(long reportingkey, MainEntryForm mef, Connection connection) throws SQLException {
	logger.debug("dbSelectReportingInd(long reportingkey, MainEntryForm mef, Connection connection)");
    //	String LR_REP_IND_Cols = "IND_KEY, REPORTING_KEY, DATE_KEY, IND_VALUE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    String qString = "";
    //first we grab the records from reporting_ind table that match the reporting key....
    qString = "SELECT IND_KEY, IND_VALUE FROM LIFEREPL.LR_REPORTING_IND WHERE REPORTING_KEY = " + reportingkey + " ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    while (rs.next()) {
      //grab the indicator from the indicator table that matches the ind_key, so we can figure out
      //which indicator this is....
      qString = "SELECT IND_CD FROM LIFEREPL.LR_INDICATOR WHERE IND_KEY = " + rs.getString("IND_KEY") + " ";
      rs2 = stmt2.executeQuery(qString);
      if (rs2.next()) {
        //check for each indicator, and if found, update the mef appropriately.
        if (rs2.getString("IND_CD").equalsIgnoreCase(LrtsProperties.getReportingIndicator())) {
          //the value is switched around for reporting indicator, because the prompt on the presentation says:
          //"Delete from report?" which is the exact opposite meaning from what the indicator string says.
          if (rs.getString("IND_VALUE").equalsIgnoreCase("Y")) {
            mef.setDeleteFromReport("N");
          } else {
            mef.setDeleteFromReport("Y");
          }
        }
        if (rs2.getString("IND_CD").equalsIgnoreCase(LrtsProperties.getDisclosureIndicator())) {
          mef.setDisclosed(rs.getString("IND_VALUE"));
        }
        if (rs2.getString("IND_CD").equalsIgnoreCase(LrtsProperties.getReplacementIndicator())) {
          mef.setReplacementType(rs.getString("IND_VALUE"));
        }
      }
    }
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Select transaction for this reporting key, and policy
   * keys.
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param replacingPolicykey long
   * @param replacedPolicyKey long
   * @param mef com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbSelectTransaction(
    long reportingkey,
    long replacingPolicyKey,
    long replacedPolicyKey,
    MainEntryForm mef,
    Connection connection)
    throws SQLException {
	logger.debug("dbSelectTransaction(long reportingkey, long replacingPolicyKey, long replacedPolicykey, MainEntryForm mef, Connection connection)");
    //	String LR_Transaction_Cols = "SEQUENCE_NUM, REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, TRX_CODE, PROCESS_DATE, TRANS_DATE, TRANS_AMOUNT, CASH_VALUE, "+
    //		" PAID_TO_DATE, COMPLETE_DATE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    //one "interesting" thing about the table design is that the reporting key relationship to the pair of replaced/replacing policies is
    //always one to
    String qString =
      "SELECT * FROM LIFEREPL.LR_TRANSACTION WHERE "
        + " REPORTING_KEY = "
        + reportingkey
        + " AND "
        + " REPLACING_POLICY_KEY = "
        + replacingPolicyKey
        + " AND "
        + " REPLACED_POLICY_KEY = "
        + replacedPolicyKey
        + " ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      if (rs.getString("PAID_TO_DATE") != null) {
        mef.setSurrenderDate(LrtsDateHandler.convertDateToOutputString(rs.getDate("PAID_TO_DATE")));
      }
      if (rs.getString("COMPLETE_DATE") != null) {
        mef.setTransactionDate(LrtsDateHandler.convertDateToOutputString(rs.getDate("COMPLETE_DATE")));
      }
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Update the company entry for this company key.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param companyKey java.lang.String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbUpdateCompany(
    String companyKey,
    String companyName,
    String address1,
    String address2,
    String city,
    String state,
    String zip,
    String country,
    String displayed,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbUpdateCompany(String companyKey, String companyName, String address1, String address2, String city, String state, String zip, String country, String displayed, String userid, Connection connection)");
    //	String LR_Ins_Co_Cols = "COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY_STATE, ";
    //	LR_Ins_Co_Cols += "COMPANY_ZIP, COMPANY_COUNTRY, COMPANY_SHOW_IN_LIST, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    //	LR_Ins_Co_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    // Read the old record
    String qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + companyKey + "' ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      // now see if anything has changed
      qString =
        "SELECT INSURANCE_CO_KEY FROM LIFEREPL.LR_INSURANCE_COMPANY "
          + "WHERE COMPANY_NAME "
          + wrapWithOperator(companyName)
          + " AND "
          + "COMPANY_ADDRESS_LINE_1 "
          + wrapWithOperator(address1)
          + " AND "
          + "COMPANY_ADDRESS_LINE_2 "
          + wrapWithOperator(address2)
          + " AND "
          + "COMPANY_CITY "
          + wrapWithOperator(city)
          + " AND "
          + "COMPANY_STATE "
          + wrapWithOperator(state)
          + " AND  "
          + "COMPANY_ZIP "
          + wrapWithOperator(zip)
          + " AND "
          + "COMPANY_COUNTRY "
          + wrapWithOperator(country)
          + " AND "
          + "COMPANY_SHOW_IN_LIST "
          + wrapWithOperator(displayed)
          + " AND "
          + " INSURANCE_CO_KEY = '"
          + companyKey
          + "' ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { // If record with all the current values does not exist, which means it should be updated.
        // Create a Company History record.
        dbInsertCompanyHistory(
          companyKey,
          rs.getString("COMPANY_NAME"),
          rs.getString("COMPANY_ADDRESS_LINE_1"),
          rs.getString("COMPANY_ADDRESS_LINE_2"),
          rs.getString("COMPANY_CITY"),
          rs.getString("COMPANY_STATE"),
          rs.getString("COMPANY_ZIP"),
          rs.getString("COMPANY_COUNTRY"),
          rs.getString("COMPANY_SHOW_IN_LIST"),
          userid,
          connection);
        // Set up the Insurance Company Update.
        qString = "UPDATE LIFEREPL.LR_INSURANCE_COMPANY SET ";
        qString += "COMPANY_NAME = "
          + wrap(companyName)
          + ", "
          + "COMPANY_ADDRESS_LINE_1 = "
          + wrap(address1)
          + ", "
          + "COMPANY_ADDRESS_LINE_2 = "
          + wrap(address2)
          + ", "
          + "COMPANY_CITY = "
          + wrap(city)
          + ", "
          + "COMPANY_STATE = "
          + wrap(state)
          + ", "
          + "COMPANY_ZIP = "
          + wrap(zip)
          + ", "
          + "COMPANY_COUNTRY = "
          + wrap(country)
          + ", "
          + "COMPANY_SHOW_IN_LIST = "
          + wrap(displayed)
          + ", "
          + "UPDATE_ROW_PGMNAME = 'LRTSUPCO', UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = "
          + wrap(userid)
          + " "
          + " WHERE INSURANCE_CO_KEY = '"
          + companyKey
          + "' ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { // if no record was updated
          try {
            rs2.close();
          } catch (Exception ignore) {}
          try {
            rs.close();
          } catch (Exception ignore) {}
          try {
            stmt2.close();
          } catch (Exception ignore) {}
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Company.  Company record not found.");
        }
      } // If record with all the current values does not exist, which means it should be updated.
      else {
        // do nothing, record does not need to be updated.
      }
    } else { // record to update was not found.
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Company.  Company record not found.");
    }
    // Cleanup...
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Update the policy entry.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @param policykey long
   * @param plancode java.lang.String
   * @param state java.lang.String
   * @param product java.lang.String
   * @param company java.lang.String
   * @param policynumber java.lang.String
   * @param lastname java.lang.String
   * @param firstname java.lang.String
   * @param middleinitial java.lang.String
   * @param suffix java.lang.String
   * @param agentcode java.lang.String
   * @param district java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbUpdatePolicy(
    long policykey,
    String plancode,
    String state,
    String product,
    String company,
    String policynumber,
    String lastname,
    String firstname,
    String middleinitial,
    String suffix,
    String agentcode,
    String district,
    String userid,
    Connection connection)
    throws SQLException {
    //database changes incorporated
	logger.debug("dbUpdatePolicy(long policykey, String plancode, String state, String product, String company, String policynumber, String lastname, String firstname, String middleinitial, String suffix, String agentcode, String district, String userid, Connection connection)");
    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    // these "if"s added on 2/18/03 to comply with change to default certain fields.  Appears to be needed for reporting
    // side of system functionality....  C. Pauer - TeamSoft Inc. - 2/18/03
    if ((plancode == null) || (plancode.trim().equals(""))) {
      plancode = LrtsProperties.getDefaultPlanCode();
    }
    if ((agentcode == null) || (agentcode.trim().equals(""))) {
      agentcode = LrtsProperties.getDefaultAgent();
    }
    if ((district == null) || (district.trim().equals(""))) {
      district = LrtsProperties.getDefaultDistrict();
    }
    if ((state == null) || (state.trim().equals(""))) {
      state = LrtsProperties.getDefaultStateCode();
    }
    if ((product == null) || (product.trim().equals(""))) {
      product = LrtsProperties.getDefaultProductType();
    }
    ////////////////////////////////
    // read old record
    String qString = "SELECT * FROM LIFEREPL.LR_POLICY_GEN WHERE POLICY_KEY = '" + policykey + "' ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      //now see if anything has changed
      qString =
        "SELECT POLICY_KEY FROM LIFEREPL.LR_POLICY_GEN WHERE "
          + "PLAN_CODE_KEY "
          + wrapWithOperator(plancode)
          + " AND "
          + "CONTRACT_STATE_ID "
          + wrapWithOperator(state)
          + " AND "
          + "PRODUCT_TYPE_KEY "
          + wrapWithOperator(product)
          + " AND "
          + "INSURANCE_CO_KEY "
          + wrapWithOperator(company)
          + " AND "
          + "OWNER_LAST_NAME "
          + wrapWithOperator(lastname)
          + " AND "
          + "OWNER_FIRST_NAME "
          + wrapWithOperator(firstname)
          + " AND "
          + "OWNER_MI "
          + wrapWithOperator(middleinitial)
          + " AND "
          + "OWNER_SUFFIX "
          + wrapWithOperator(suffix)
          + " AND "
          + "AGT_CD "
          + wrapWithOperator(agentcode)
          + " AND "
          + "DIST_CD "
          + wrapWithOperator(district)
          + " AND "
          + "POLICY_KEY = '"
          + policykey
          + "' ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { //if record with all the current values does not exist, which means it should be updated.
        String oDOB = null;
        String iDOB = null;
        String polEffDate = null;
        if (rs.getDate("OWNER_DOB") != null) {
          oDOB = LrtsDateHandler.convertDateToSaveString(rs.getDate("OWNER_DOB"));
        }
        if (rs.getDate("INSURED_DOB") != null) {
          iDOB = LrtsDateHandler.convertDateToSaveString(rs.getDate("INSURED_DOB"));
        }
        if (rs.getDate("POLICY_EFF_DATE") != null) {
          polEffDate = LrtsDateHandler.convertDateToSaveString(rs.getDate("POLICY_EFF_DATE"));
        }
        dbInsertPolicyHistory(
          Long.toString(policykey),
          rs.getString("DATE_KEY"),
          rs.getString("PLAN_CODE_KEY"),
          rs.getString("CONTRACT_STATE_ID"),
          rs.getString("PRODUCT_TYPE_KEY"),
          rs.getString("INSURANCE_CO_KEY"),
          rs.getString("POLICY_NUMBER"),
          rs.getString("OWNER_LAST_NAME"),
          rs.getString("OWNER_FIRST_NAME"),
          rs.getString("OWNER_MI"),
          rs.getString("OWNER_SUFFIX"),
          oDOB,
          rs.getString("OWNER_SSN"),
          rs.getString("INSURED_LAST_NAME"),
          rs.getString("INSURED_FIRST_NAME"),
          rs.getString("INSURED_MI"),
          rs.getString("INSURED_SUFFIX"),
          iDOB,
          rs.getString("INSURED_SSN"),
          rs.getString("AGT_CD"),
          rs.getString("DIST_CD"),
          rs.getString("AGT_ID"),
          rs.getString("SERVICING_AGT_CD"),
          rs.getString("SERVICING_DIST_CD"),
          rs.getString("SERVICING_AGT_ID"),
          rs.getString("AMOUNT_OF_INS"),
          rs.getString("POLICY_STATUS"),
          rs.getString("REPLACEMENT_IND"),
          polEffDate,
          userid,
          connection);
        qString =
          "UPDATE LIFEREPL.LR_POLICY_GEN SET "
            + "PLAN_CODE_KEY = "
            + wrap(plancode)
            + ", "
            + "CONTRACT_STATE_ID = "
            + wrap(state)
            + ", "
            + "PRODUCT_TYPE_KEY = "
            + wrap(product)
            + ", "
            + "INSURANCE_CO_KEY = "
            + wrap(company)
            + ", "
            + "OWNER_LAST_NAME = "
            + wrap(lastname)
            + ", "
            + "OWNER_FIRST_NAME = "
            + wrap(firstname)
            + ", "
            + "OWNER_MI = "
            + wrap(middleinitial)
            + ", "
            + "OWNER_SUFFIX = "
            + wrap(suffix)
            + ", "
            + "AGT_CD = "
            + wrap(agentcode)
            + ", "
            + "DIST_CD = "
            + wrap(district)
            + ", "
            + "UPDATE_ROW_PGMNAME = 'LRTSUPPO', "
            + "UPDATE_ROW_TS = SYSDATE, "
            + "UPDATE_ROW_USERID = "
            + wrap(userid)
            + " "
            + " WHERE POLICY_KEY = '"
            + policykey
            + "' ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { //if no record was updated
          try {
            rs2.close();
          } catch (Exception ignore) {}
          try {
            rs.close();
          } catch (Exception ignore) {}
          try {
            stmt2.close();
          } catch (Exception ignore) {}
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Policy.  Policy record not found.");
        }
      } else {
        // do nothing, record with these values already exists so nothing on presentation has changed.
      }
    } else { // record to update was not found.
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Company.  Company record not found.");
    }
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Update report entry
   * Creation date: (1/2/2003 3:08:36 PM)
   * @param reportingKey long
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbUpdateReport(long reportingKey, String userid, Connection connection) throws SQLException {
    //This routine may not be called.  At this point, reporting table entries never change for a replacing/replaced
    //policy pair, so updates to this table are not happening.  Still, here is the code....
	logger.debug("dbUpdateReport(long reportingKey, String userid, Connection connection)");
    //	String LR_Reporting_Cols = "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String qString =
      "UPDATE LIFEREPL.LR_REPORTING SET UPDATE_ROW_PGMNAME = 'LRTSUPRP', UPDATE_ROW_TS = SYSDATE, "
        + "UPDATE_ROW_USERID = "
        + wrap(userid)
        + " WHERE REPORTING_KEY = "
        + reportingKey
        + " ";
    logger.debug(qString);
    if (stmt.executeUpdate(qString) == 0) {
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Report Entry.  Report Entry record not found.");
    }
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Update reporting indicators
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param mef com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbUpdateReportingInd(long reportingkey, MainEntryForm mef, Connection connection) throws SQLException {
    //even though this is titled an Update method, there insert methods called if a new indicator has been filled in.
	logger.debug("dbUpdateReportingInd(long reportingkey, MainEntryForm mef, Connection connection)");
    String LR_REP_IND_Cols = "IND_KEY, REPORTING_KEY, IND_VALUE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    ResultSet rs = null;
    ResultSet rs2 = null;
    String qString = "";
    String holder = "";
    //each if structure checks for a different indicator on the MainEntryForm
    if ((mef.getDisclosed() != null) && !(mef.getDisclosed().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE "
          + " OR END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getDisclosureIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        holder = rs.getString("IND_KEY");
        qString = "SELECT * FROM LIFEREPL.LR_REPORTING_IND WHERE IND_KEY = '" + holder + "' AND REPORTING_KEY='" + reportingkey + "'  ";
        rs2 = stmt.executeQuery(qString);
        if (rs2.next()) {
          if ((rs2.getString("IND_VALUE") == null) || !(rs2.getString("IND_VALUE").equals(mef.getDisclosed()))) {
            //history insert
            dbInsertReportingIndHistory(
              rs2.getString("IND_KEY"),
              rs2.getString("REPORTING_KEY"),
              rs2.getString("DATE_KEY"),
              rs2.getString("IND_VALUE"),
              mef.getUserId(),
              connection);
            //update
            qString =
              "UPDATE LIFEREPL.LR_REPORTING_IND SET IND_VALUE = '"
                + mef.getDisclosed()
                + "', UPDATE_ROW_PGMNAME = 'LRTSUPRI',  "
                + " UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = '"
                + mef.getUserId()
                + "' WHERE IND_KEY = '"
                + holder
                + "' AND REPORTING_KEY='"
                + reportingkey
                + "'  ";
            logger.debug(qString);
            if (stmt.executeUpdate(qString) == 0) {
              try {
                rs2.close();
              } catch (Exception ignore) {}
              try {
                rs.close();
              } catch (Exception ignore) {}
              try {
                stmt.close();
              } catch (Exception ignore) {}
              throw new SQLException("No update performed on Report Indicators.  Report Indicators record not found.");
            }
          }
        } else {
          //insert
          qString =
            "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
              + LR_REP_IND_Cols
              + ") VALUES ("
              + "'"
              + holder
              + "', '"
              + reportingkey
              + "', "
              + getDateKey(connection)
              + ",  '"
              + mef.getDisclosed()
              + "', 'LRTSINRI', SYSDATE, '"
              + mef.getUserId()
              + "')";
          logger.debug(qString);
          if (stmt.executeUpdate(qString) == 0) {
            try {
              rs2.close();
            } catch (Exception ignore) {}
            try {
              rs.close();
            } catch (Exception ignore) {}
            try {
              stmt.close();
            } catch (Exception ignore) {}
            throw new SQLException("No insert performed on Report Indicators.  Report Indicators record could not be inserted.");
          }
        }
      }
    }
    if ((mef.getDeleteFromReport() != null) && !(mef.getDeleteFromReport().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE  OR "
          + " END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getReportingIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      String reportString = "";
      //because the screen prompt says "delete from report?" and the indicator is a "reporting indicator", the value has to be
      //flipped to stay consistent with the area it is presented under.
      if (mef.getDeleteFromReport().trim().equalsIgnoreCase("Y")) {
        reportString = "N";
      } else {
        reportString = "Y";
      }
      if (rs.next()) {
        holder = rs.getString("IND_KEY");
        qString = "SELECT * FROM LIFEREPL.LR_REPORTING_IND WHERE IND_KEY = '" + holder + "' AND REPORTING_KEY='" + reportingkey + "'  ";
        rs2 = stmt.executeQuery(qString);
        if (rs2.next()) {
          //update
          if ((rs2.getString("IND_VALUE") == null) || !(rs2.getString("IND_VALUE").equals(reportString))) {
            //history insert
            dbInsertReportingIndHistory(
              rs2.getString("IND_KEY"),
              rs2.getString("REPORTING_KEY"),
              rs2.getString("DATE_KEY"),
              rs2.getString("IND_VALUE"),
              mef.getUserId(),
              connection);
            qString =
              "UPDATE LIFEREPL.LR_REPORTING_IND SET IND_VALUE = '"
                + reportString
                + "', UPDATE_ROW_PGMNAME = 'LRTSUPRI',  "
                + " UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = '"
                + mef.getUserId().toUpperCase()
                + "' WHERE IND_KEY = '"
                + holder
                + "' AND REPORTING_KEY='"
                + reportingkey
                + "'  ";
            logger.debug(qString);
            if (stmt.executeUpdate(qString) == 0) {
              try {
                rs2.close();
              } catch (Exception ignore) {}
              try {
                rs.close();
              } catch (Exception ignore) {}
              try {
                stmt.close();
              } catch (Exception ignore) {}
              throw new SQLException("No update performed on Report Indicators.  Report Indicators record not found.");
            }
          }
        } else {
          qString =
            "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
              + LR_REP_IND_Cols
              + ") VALUES ("
              + "'"
              + holder
              + "', '"
              + reportingkey
              + "', "
              + getDateKey(connection)
              + ",   '"
              + reportString
              + "', 'LRTSINRI', SYSDATE, '"
              + mef.getUserId().toUpperCase()
              + "')";
          logger.debug(qString);
          if (stmt.executeUpdate(qString) == 0) {
            try {
              rs2.close();
            } catch (Exception ignore) {}
            try {
              rs.close();
            } catch (Exception ignore) {}
            try {
              stmt.close();
            } catch (Exception ignore) {}
            throw new SQLException("No insert performed on Report Indicators.  Report Indicators record could not be inserted.");
          }
        }
      }
    }
    if ((mef.getReplacementType() != null) && !(mef.getReplacementType().trim().equals(""))) {
      qString =
        "SELECT IND_KEY FROM LIFEREPL.LR_INDICATOR WHERE SYSDATE > EFF_DATE AND (SYSDATE < END_EFF_DATE "
          + " OR END_EFF_DATE IS null) AND "
          + "  IND_CD = '"
          + LrtsProperties.getReplacementIndicator()
          + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        holder = rs.getString("IND_KEY");
        qString = "SELECT * FROM LIFEREPL.LR_REPORTING_IND WHERE IND_KEY = '" + holder + "' AND REPORTING_KEY='" + reportingkey + "'  ";
        rs2 = stmt.executeQuery(qString);
        if (rs2.next()) {
          //update
          if ((rs2.getString("IND_VALUE") == null) || !(rs2.getString("IND_VALUE").equals(mef.getReplacementType()))) {
            //history insert
            dbInsertReportingIndHistory(
              rs2.getString("IND_KEY"),
              rs2.getString("REPORTING_KEY"),
              rs2.getString("DATE_KEY"),
              rs2.getString("IND_VALUE"),
              mef.getUserId(),
              connection);
            qString =
              "UPDATE LIFEREPL.LR_REPORTING_IND SET IND_VALUE = '"
                + mef.getReplacementType()
                + "', UPDATE_ROW_PGMNAME = 'LRTSUPRI',  "
                + " UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = '"
                + mef.getUserId().toUpperCase()
                + "' WHERE IND_KEY = '"
                + holder
                + "' AND REPORTING_KEY='"
                + reportingkey
                + "'  ";
            logger.debug(qString);
            if (stmt.executeUpdate(qString) == 0) {
              try {
                rs2.close();
              } catch (Exception ignore) {}
              try {
                rs.close();
              } catch (Exception ignore) {}
              try {
                stmt.close();
              } catch (Exception ignore) {}
              throw new SQLException("No update performed on Report Indicators.  Report Indicators record not found.");
            }
          }
        } else {
          qString =
            "INSERT INTO LIFEREPL.LR_REPORTING_IND ("
              + LR_REP_IND_Cols
              + ") VALUES ("
              + "'"
              + holder
              + "', '"
              + reportingkey
              + "', "
              + getDateKey(connection)
              + ",   '"
              + mef.getReplacementType()
              + "', 'LRTSINRI', SYSDATE, '"
              + mef.getUserId().toUpperCase()
              + "')";
          logger.debug(qString);
          if (stmt.executeUpdate(qString) == 0) {
            try {
              rs2.close();
            } catch (Exception ignore) {}
            try {
              rs.close();
            } catch (Exception ignore) {}
            try {
              stmt.close();
            } catch (Exception ignore) {}
            throw new SQLException("No insert performed on Report Indicators.  Report Indicators record could not be inserted.");
          }
        }
      }
    }
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Update the transaction record
   * Creation date: (1/2/2003 3:10:38 PM)
   * @param reportingkey long
   * @param replacingPolicyKey long
   * @param replacedPolicyKey long
   * @param surrenderDate java.lang.String
   * @param transDate java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void dbUpdateTransaction(
    long reportingkey,
    long replacingPolicyKey,
    long replacedPolicyKey,
    String surrenderDate,
    String transDate,
    String userid,
    Connection connection)
    throws SQLException {
	logger.debug("dbUpdateTransaction(long reportingkey, long replacingPolicyKey, long replacedPolicykey, String surrenderDate, String transDate, String userid, Connection connection)");
    //	String LR_Transaction_Cols = "REPORTING_KEY, REPLACING_POLICY_KEY, REPLACED_POLICY_KEY, DATE_KEY, TRX_CODE, PROCESS_DATE, TRANS_DATE, TRANS_AMOUNT, CASH_VALUE, "+
    //		" PAID_TO_DATE, COMPLETE_DATE, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((surrenderDate != null) && (!(surrenderDate.trim().equals("")))) {
      try {
        if (surrenderDate.length() > 10) {
          surrenderDate = surrenderDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(surrenderDate)) {
          surrenderDate = LrtsDateHandler.convertWebDateStringToPersistString(surrenderDate);
        } else {
          throw new SQLException("Date Exception: Surrender Date : " + surrenderDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }
    if ((transDate != null) && (!(transDate.trim().equals("")))) {
      try {
        if (transDate.length() > 10) {
          transDate = transDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(transDate)) {
          transDate = LrtsDateHandler.convertWebDateStringToPersistString(transDate);
        } else {
          throw new SQLException("Date Exception: Trans Date : " + transDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }
    //read old record
    String qString =
      "SELECT * FROM LIFEREPL.LR_TRANSACTION WHERE REPORTING_KEY = "
        + reportingkey
        + " "
        + " AND REPLACING_POLICY_KEY = "
        + replacingPolicyKey
        + " "
        + " AND REPLACED_POLICY_KEY = "
        + replacedPolicyKey
        + " ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      //now see if anything has changed
      qString =
        "SELECT REPORTING_KEY FROM LIFEREPL.LR_TRANSACTION WHERE "
          + "PAID_TO_DATE "
          + wrapWithOperator(surrenderDate)
          + " AND "
          + "COMPLETE_DATE "
          + wrapWithOperator(transDate)
          + " AND "
          + " REPORTING_KEY = "
          + reportingkey
          + " AND "
          + " REPLACING_POLICY_KEY = "
          + replacingPolicyKey
          + " AND "
          + " REPLACED_POLICY_KEY = "
          + replacedPolicyKey
          + " ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { //if record with all the current values does not exist, which means it should be updated.
        String processDate = null;
        String completeDate = null;
        String rtransDate = null;
        String paidToDate = null;
        if (rs.getDate("PROCESS_DATE") != null) {
          processDate = LrtsDateHandler.convertDateToSaveString(rs.getDate("PROCESS_DATE"));
        }
        if (rs.getDate("COMPLETE_DATE") != null) {
          completeDate = LrtsDateHandler.convertDateToSaveString(rs.getDate("COMPLETE_DATE"));
        }
        if (rs.getDate("TRANS_DATE") != null) {
          rtransDate = LrtsDateHandler.convertDateToSaveString(rs.getDate("TRANS_DATE"));
        }
        if (rs.getDate("PAID_TO_DATE") != null) {
          paidToDate = LrtsDateHandler.convertDateToSaveString(rs.getDate("PAID_TO_DATE"));
        }
        dbInsertTransactionHistory(
          rs.getString("TRANSACTION_KEY"),
          rs.getString("DATE_KEY"),
          rs.getString("REPORTING_KEY"),
          rs.getString("REPLACING_POLICY_KEY"),
          rs.getString("REPLACED_POLICY_KEY"),
          rs.getString("TRX_CODE"),
          processDate,
          rtransDate,
          rs.getString("TRANS_AMOUNT"),
          rs.getString("CASH_VALUE"),
          paidToDate,
          completeDate,
          userid,
          connection);
        qString =
          "UPDATE LIFEREPL.LR_TRANSACTION SET PAID_TO_DATE = "
            + wrap(surrenderDate)
            + ", "
            + "COMPLETE_DATE = "
            + wrap(transDate)
            + ", "
            + "UPDATE_ROW_PGMNAME = 'LRTSUPTR', "
            + "UPDATE_ROW_TS = SYSDATE, "
            + "UPDATE_ROW_USERID = "
            + wrap(userid)
            + " "
            + " WHERE REPORTING_KEY = "
            + reportingkey
            + " "
            + " AND REPLACING_POLICY_KEY = "
            + replacingPolicyKey
            + " "
            + " AND REPLACED_POLICY_KEY = "
            + replacedPolicyKey
            + " ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { //if no record was updated.
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Transaction.  Transaction record not found.");
        }
      } else {
        //do nothing: record with these entered values is already on persistence and should not be updated.
      }
    } else { //no record found to update....
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Transaction.  Transaction record not found.");
    }
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Get authorization level, as int, based on department name.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @return int
   * @param departmentName java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  int getAuthorizationLevel(String departmentName, Connection connection) throws SQLException {
	logger.debug("getAuthorizationLevel(String departmentName, Connection connection)");
    boolean ok = true;
    int returnVal = -1;
    //try block
    try {
      //set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      if ((departmentName != null) && !(departmentName.trim().equals(""))) {
        returnVal = dbSelectAuthorization(departmentName, connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.debug("enzo's test for dept name: " + departmentName);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return returnVal;
  }
  private void dbDeleteAuthorizedUser(String parmUserId, Connection parmConnection) throws SQLException {
    String theSQL = "DELETE FROM LIFEREPL.LR_ADMIN_AUTH WHERE USERID = '" + parmUserId + "'" ;
	if (executeUpdate(theSQL,parmConnection) != 1) throw new SQLException("No rows affected: " + theSQL) ;
  }
    private ResultSet executeQuery(String parmSQL, Connection parmConnection) throws SQLException {
      boolean theOldAutoCommit = parmConnection.getAutoCommit();
      ResultSet theResult = null;
      try {
        if (!theOldAutoCommit) {
          parmConnection.commit();
          parmConnection.setAutoCommit(true);
        }
        theResult = parmConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY).executeQuery(parmSQL);
      } finally {
        if (!theOldAutoCommit)
              parmConnection.setAutoCommit(theOldAutoCommit);
        return theResult;
      }
    }
                private int executeUpdate(String parmSQL, Connection parmConnection) throws SQLException {
                  boolean theOldAutoCommit = parmConnection.getAutoCommit();
                  Statement theStatement = null;
                  try {
                    if (!theOldAutoCommit) {
                      parmConnection.commit();
                      parmConnection.setAutoCommit(true);
                    }
                    theStatement = parmConnection.createStatement();
                    System.out.println("Executing: " + parmSQL) ;
                    return theStatement.executeUpdate(parmSQL);
                  } finally {
                    if (theStatement != null) {
                      try {
                        theStatement.close();
                      } catch (Throwable eatit) {}
                      theStatement = null;
                    }
                    if (!theOldAutoCommit)
                                      parmConnection.setAutoCommit(theOldAutoCommit);
                  }
                }
    private void closeResultSet(ResultSet parmResults) {
    		try {
	    		Statement theStatement = parmResults.getStatement() ;
	    		parmResults.close() ;
	    		parmResults = null ;
	    		theStatement.close() ;
	    		theStatement = null ;
    		} catch (Throwable t) {
    		}
    }

  private void dbInsertAuthorizedUser(String parmUserId, String parmName, Connection parmConnection) throws SQLException {
	String theSQL = "INSERT INTO LIFEREPL.LR_ADMIN_AUTH (USERID,USER_NAME) VALUES ('" + parmUserId + "','" + parmName + "')";
    if (executeUpdate(theSQL,parmConnection) != 1) throw new SQLException("No rows affected for: " + theSQL) ;
  }
  private void dbUpdateContractState(String parmCode, String parmName, boolean parmAFLIC, Connection parmConnection) throws SQLException {
  	String theAFLIC = (parmAFLIC)? "Y":"N" ;
	String theSQL = "UPDATE LIFEREPL.LR_CONTRACT_STATE SET STATE_NAME='" + parmName + "', AFLIC_STATE_IND='" + theAFLIC + "' WHERE STATE_CODE = '" + parmCode + "'";
    if (executeUpdate(theSQL,parmConnection) != 1) throw new SQLException("No rows affected for: " + theSQL) ;
  }
  private ResultSet dbSelectAuthorizedUser(Connection parmConnection) throws SQLException {
  	return executeQuery("SELECT USERID,USER_NAME FROM LIFEREPL.LR_ADMIN_AUTH ORDER BY USERID",parmConnection);
  }
  private ResultSet dbSelectContractState(Connection parmConnection) throws SQLException {
  	return executeQuery("SELECT STATE_CODE,STATE_NAME,AFLIC_STATE_IND FROM LIFEREPL.LR_CONTRACT_STATE ORDER BY STATE_CODE",parmConnection);
  }
  /**
   * Checks to see if the user is authorized as a table administrator.
   * Creation date: (11/26/2003 3:25:00 PM)
   * @return boolean
   * @param userID java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  boolean isAuthorizedAsAdministrator(String userID, Connection connection) {
    // Checks to see if the user is authorized as a table administrator.
	logger.debug("isAuthorizedAsAdministrator(String userID, Connection connection)");
    boolean returnVal = false;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      String qString = "SELECT USERID FROM LIFEREPL.LR_ADMIN_AUTH WHERE USERID LIKE '" + userID.toUpperCase() + "%' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        returnVal = true;
      }
    } catch (SQLException sqle) {
      logger.error("Exception: " + sqle);
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Read appropriate Key value from the TIME table
   * based on SYSDATE.
   * Creation date: (1/22/2003 2:49:26 PM)
   * @return long
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long getDateKey(Connection connection) throws SQLException {
    logger.debug("getDateKey()");
    String qString = "";
    Statement stmt = null;
    ResultSet rs = null;
    long returnVal = -1;
    stmt = connection.createStatement();
    qString = "SELECT DATE_KEY FROM LIFEREPL.LR_TIME_MO WHERE SYSDATE >= PERIOD_ST_DT AND SYSDATE < (PERIOD_END_DT+1)";
    logger.debug(qString);
    rs = stmt.executeQuery(qString);
    if (rs.next()) {
      returnVal = rs.getLong("DATE_KEY");
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Checks to see if companyKey should be shown
   * in the company selection list.
   * Creation date: (1/2/2003 3:21:10 PM)
   * @return boolean
   * @param companyKey java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  boolean isCompanyInDisplayList(String companyKey, Connection connection) {
    //show in list not only indicates how this company name should be handled on the dropdown on the presentation, but
    //also indicates, if N, that the company record here was entered by the user as non-pre-existing entry in the
    //company table.  As such, it is only associated with the policy_gen record that indexes it, it is not used again, even
    //if the exact same company is entered by another user.
	logger.debug("isCompanyInDisplayList(String companyKey,  Connection connection)");
    boolean returnVal = false;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      String qString = "SELECT COMPANY_SHOW_IN_LIST FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + companyKey + "' ";
      logger.debug(qString);
      rs = stmt.executeQuery(qString);
      if (rs.next()) {
        if (rs.getString("COMPANY_SHOW_IN_LIST").trim().equals("Y")) {
          returnVal = true;
        }
      }
    } catch (SQLException sqle) {
      logger.error("Exception: " + sqle);
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Select routine for InsuranceCompanyUpdateForm object.
   * @param param com.amfam.lrts.form.InsuranceCompanyUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  InsuranceCompanyUpdateForm insuranceCompanyUpdateFormSelect(InsuranceCompanyUpdateForm param, Connection connection)
    throws SQLException {
    // this is a full select of the insurance company update form.  It is expected that the insurance company key, if found, can be used to fill the
    // form....
	logger.debug("insuranceCompanyUpdateFormSelect(InsuranceCompanyUpdateForm param, Connection connection)");
    boolean ok = true;
    // try block
    try {
      // set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      if (param.getRecordKey() != null) {
        dbSelectCompanies(param, connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }
  /**
   * Selects the company information and adds it to the InsuranceCompanyUpdateForm
   * @param icu com.amfam.lrts.form.InsuranceCompanyUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  private void dbSelectCompanies(InsuranceCompanyUpdateForm icu, Connection connection) throws SQLException {
	logger.debug("dbSelectCompanies(InsuranceCompanyUpdateForm icu, Connection connection)");
    //	String LR_Ins_Co_Cols = "COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY STATE, ";
    //	LR_Ins_Co_Cols += "COMPANY_ZIP, COMPANY_COUNTRY, COMPANY_SHOW_IN_LIST, CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    //	LR_Ins_Co_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    ResultSet rs = null;
    // Build the SELECT statement.
    String qString = "SELECT * FROM LIFEREPL.LR_INSURANCE_COMPANY WHERE INSURANCE_CO_KEY = '" + icu.getRecordKey() + "' ";
    // Log the SELECT statement.
    logger.debug(qString);
    // Execute the SELECT statement.
    rs = stmt.executeQuery(qString);
    if (rs.next()) {
      if (rs.getString("COMPANY_NAME") != null) {
        icu.setCompanyName(rs.getString("COMPANY_NAME"));
      }
      if (rs.getString("COMPANY_ADDRESS_LINE_1") != null) {
        icu.setAddress1(rs.getString("COMPANY_ADDRESS_LINE_1"));
      }
      if (rs.getString("COMPANY_ADDRESS_LINE_2") != null) {
        icu.setAddress2(rs.getString("COMPANY_ADDRESS_LINE_2"));
      }
      if (rs.getString("COMPANY_CITY") != null) {
        icu.setCity(rs.getString("COMPANY_CITY"));
      }
      if (rs.getString("COMPANY_STATE") != null) {
        icu.setState(rs.getString("COMPANY_STATE"));
      }
      if (rs.getString("COMPANY_COUNTRY") != null) {
        icu.setCountry(rs.getString("COMPANY_COUNTRY"));
      }
      if (rs.getString("COMPANY_SHOW_IN_LIST") != null) {
        icu.setDisplayed(rs.getString("COMPANY_SHOW_IN_LIST"));
      }
      if (rs.getString("COMPANY_ZIP") != null) {
        icu.setZip(rs.getString("COMPANY_ZIP"));
      }
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }
  /**
   * Do update on data in Insurance Company Update form.
   * Creation date: (11/21/2003 9:30:00 PM)
   * @param param com.amfam.lrts.form.InsuranceCompanyUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException The exception description.
   */
  void insuranceCompanyUpdateFormUpdate(InsuranceCompanyUpdateForm param, Connection connection) throws SQLException {
	logger.debug("insuranceCompanyUpdateFormUpdate(InsuranceCompanyUpdateForm param, Connection connection)");
    boolean ok = true;
    String insuranceCompanyKey = "";
    try {
      // Set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      if ((param.getRecordKey() == null) || (param.getRecordKey().equals(""))) {
        // This is a new Insurance Company record. Create it...
        insuranceCompanyKey =
          dbInsertNewCompany(
            param.getCompanyName(),
            param.getAddress1(),
            param.getAddress2(),
            param.getCity(),
            param.getState(),
            param.getZip(),
            param.getCountry(),
            param.getDisplayed(),
            param.getUserId(),
            connection);
      } else {
        // This is an existing Insurance Company record. Update it...
        dbUpdateCompany(
          param.getRecordKey(),
          param.getCompanyName(),
          param.getAddress1(),
          param.getAddress2(),
          param.getCity(),
          param.getState(),
          param.getZip(),
          param.getCountry(),
          param.getDisplayed(),
          param.getUserId(),
          connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
  }


// ====================== plan code code ===================================
  /**
   * Select routine for PlanCodeUpdateForm object.
   * @param param com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  PlanCodeUpdateForm PlanCodeUpdateFormSelect(PlanCodeUpdateForm param, Connection connection)
    throws SQLException {
    // this is a full select of the insurance company update form.  It is expected that the insurance company key, if found, can be used to fill the
    // form....
	logger.debug("PlanCodeUpdateFormSelect(PlanCodeUpdateForm param, Connection connection)");
    boolean ok = true;
    // try block
    try {
      // set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      if (param.getRecordKey() != null) {
   	   // dbSelectProductType(param, connection);
        dbSelectPlanCode(param, connection);

      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }
  /**
   * Selects the company information and adds it to the PlanCodeUpdateForm
   * @param icu com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
void PlanCodeUpdateFormUpdate(PlanCodeUpdateForm param, Connection connection) throws SQLException {
	logger.debug("PlanCodeUpdateFormNew(PlanCodeUpdateForm param, Connection connection)");
    boolean ok = true;
    String planCodeKey = "";
    String productTypeKey= "";
    long productTypeSecndkey;

    try {
      // Set up connection and values
      connection.setAutoCommit(false);
      String qString = "";



      if ((param.getNewProdType()) !=null){
      	//gets the next product type key from the table
      	dbSelectProductTypeKey (param,connection);

      	//System.out.println("newProdType is not null and adding new prod type now " + param.getNewProdType() );
       	productTypeKey =
          dbInsertNewProductType(
            param.getProdTypePrimKey(),
            param.getNewProdType(),
            param.getCurrInd(),
            param.getUserId(),
            connection);


      //System.out.println("out of insertnewprodtype: |" +  param.getProdTypePrimKey() + "|");
      }

     	 //set the product type secondary key
    	  productTypeSecndkey=param.getProdTypePrimKey();
    	  //System.out.println("secondary key: |" +  productTypeSecndkey + "|");

      if ((param.getRecordKey() == null) || (param.getRecordKey().equals(""))  && (productTypeSecndkey) > 0) {

        // This is a new Plan Code with an NEW product type record. Create it...
      	//System.out.println("in dbInsertNewPlanCode: |" +  productTypeKey + "|");
      	dbSelectPlanCodeKey(param,connection);

      	planCodeKey =
          dbInsertNewPlanCodeNewProdType(
            param.getPlanCodePrimKey(),
            productTypeSecndkey,
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);

      dbSelectPlanCodeKey(param,connection);
      	planCodeKey =
          dbInsertNewPlanCodeNewProdTypeNo(
            param.getPlanCodePrimKey(),
            productTypeSecndkey,
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);


      } else if ((param.getRecordKey() == null) || (param.getRecordKey().equals("")))  {

        // This is a new Plan Code with an EXISTING product type record. Create it...
      	//System.out.println("in dbInsertNewPlanCode: |" +  productTypeKey + "|");
      	dbSelectPlanCodeKey(param,connection);
        planCodeKey =
          dbInsertNewPlanCode(
            param.getPlanCodePrimKey(),
            param.getProductType(),
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);

        dbSelectPlanCodeKey(param,connection);
        planCodeKey =
          dbInsertNewPlanCodeNo(
            param.getPlanCodePrimKey(),
            param.getProductType(),
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);

      } else if ((productTypeSecndkey) > 0) {
        // This is an existing Plan code with a NEW product type record. Update it...
        //System.out.println("in dbUpdatePlanCode: |" +  productTypeKey + "|");
        dbUpdatePlanCode(
        	param.getRecordKey(),
          //  param.getProductType(),
            productTypeSecndkey,
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);
      } else {
      	// This is an existing plan code being updated with an existing product type record. Update it...
        //System.out.println("in dbUpdatePlanCode: |" +  productTypeKey + "|");
        dbUpdatePlanCodeExtngProdType(
        	param.getRecordKey(),
            param.getProductType(),
            //productTypeSecndkey,
            param.getPlanCode(),
            param.getDisplayed(),
            param.getStartDate(),
            param.getEndDate(),
            param.getUserId(),
            connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
  }


   /**
   * Inserts a new company entry.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewPlanCode(
    long planCodePrimKey,
    String productType,
	String planCode,
	String displayed,
	String startDate,
	String endDate,
	String userid,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewPlanCode(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }



    String displayedYes="Y";
    String LR_Pln_Cod_Cols = "PLAN_CODE_KEY,PRODUCT_TYPE_KEY,PLAN_CODE, AFLIC_CURRENT_IND, EFF_DATE,END_EFF_DATE, ";
    LR_Pln_Cod_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Pln_Cod_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String qString = "INSERT INTO LIFEREPL.LR_PLAN_CD_GEN (" + LR_Pln_Cod_Cols + ") ";
    qString += "VALUES ("
      + planCodePrimKey
      + ", "
      + wrap(productType)
      + ", "
      + wrap(planCode)
      + ", "
      + wrap(displayedYes)
      + ", "
      + wrap(startDate)
      + ", "
      + wrap(endDate)
      + ", "
      + "'LRTSPLCO', SYSDATE, "
      + wrap(userid)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PLAN CODE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;  }


   /**
   * Inserts a new company entry.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewPlanCodeNo(
    long planCodePrimKey,
    String productType,
	String planCode,
	String displayed,
	String startDate,
	String endDate,
	String userid,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewPlanCode(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }



    String displayedYes="N";
    String LR_Pln_Cod_Cols = "PLAN_CODE_KEY,PRODUCT_TYPE_KEY,PLAN_CODE, AFLIC_CURRENT_IND, EFF_DATE,END_EFF_DATE, ";
    LR_Pln_Cod_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Pln_Cod_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String qString = "INSERT INTO LIFEREPL.LR_PLAN_CD_GEN (" + LR_Pln_Cod_Cols + ") ";
    qString += "VALUES ("
      + planCodePrimKey
      + ", "
      + wrap(productType)
      + ", "
      + wrap(planCode)
      + ", "
      + wrap(displayedYes)
      + ", "
      + wrap(startDate)
      + ", "
      + wrap(endDate)
      + ", "
      + "'LRTSPLCO', SYSDATE, "
      + wrap(userid)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PLAN CODE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;  }


 /**
   * Inserts a new company entry.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewPlanCodeNewProdType(
    long planCodePrimKey,
    long productTypeSecndkey,
	String planCode,
	String displayed,
	String startDate,
	String endDate,
	String userid,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewPlanCode(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }



    String displayedYes="Y";
    String LR_Pln_Cod_Cols = "PLAN_CODE_KEY,PRODUCT_TYPE_KEY,PLAN_CODE, AFLIC_CURRENT_IND, EFF_DATE,END_EFF_DATE, ";
    LR_Pln_Cod_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Pln_Cod_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String qString = "INSERT INTO LIFEREPL.LR_PLAN_CD_GEN (" + LR_Pln_Cod_Cols + ") ";
    qString += "VALUES ("
      + planCodePrimKey
      + ", "
      //+ wrap(productType)
      + productTypeSecndkey
      + ", "
      + wrap(planCode)
      + ", "
      + wrap(displayedYes)
      + ", "
      + wrap(startDate)
      + ", "
      + wrap(endDate)
      + ", "
      + "'LRTSPLCO', SYSDATE, "
      + wrap(userid)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PLAN CODE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;  }



/**
   * Inserts a new company entry.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @return String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewPlanCodeNewProdTypeNo(
    long planCodePrimKey,
    long productTypeSecndkey,
	String planCode,
	String displayed,
	String startDate,
	String endDate,
	String userid,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewPlanCode(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }



    String displayedYes="N";
    String LR_Pln_Cod_Cols = "PLAN_CODE_KEY,PRODUCT_TYPE_KEY,PLAN_CODE, AFLIC_CURRENT_IND, EFF_DATE,END_EFF_DATE, ";
    LR_Pln_Cod_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Pln_Cod_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String qString = "INSERT INTO LIFEREPL.LR_PLAN_CD_GEN (" + LR_Pln_Cod_Cols + ") ";
    qString += "VALUES ("
      + planCodePrimKey
      + ", "
      //+ wrap(productType)
      + productTypeSecndkey
      + ", "
      + wrap(planCode)
      + ", "
      + wrap(displayedYes)
      + ", "
      + wrap(startDate)
      + ", "
      + wrap(endDate)
      + ", "
      + "'LRTSPLCO', SYSDATE, "
      + wrap(userid)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PLAN CODE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;  }


/**
   * Update the plan code entry for this plan code key.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param companyKey java.lang.String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void  dbUpdatePlanCode(

    String planCodeKey,
    long productTypeSecndkey,
    String planCode,
    String displayed,
    String startDate,
    String endDate,
    String userId,
    Connection connection)
    throws SQLException {
	logger.debug("dbUpdatePlanCode(String planCodeKey, String productTypeKey, String displayed, String startDate, String endDate, Connection connection");

    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    // Read the old record
// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }


   //read the old record
    String qString = "SELECT * FROM LIFEREPL.LR_PLAN_CD_GEN WHERE PLAN_CODE_KEY = '" + planCodeKey + "' ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      // now see if anything has changed
      qString =
        "SELECT PLAN_CODE_KEY FROM LIFEREPL.LR_PLAN_CD_GEN "
          + "WHERE PRODUCT_TYPE_KEY "
          //+ wrapWithOperator(productType)
          + " = " + productTypeSecndkey
          + " AND "
          + "PLAN_CODE "
          + wrapWithOperator(planCode)
          + " AND "
          + "AFLIC_CURRENT_IND "
          + wrapWithOperator(displayed)
          + " AND "
          + "EFF_DATE "
          + wrapWithOperator(startDate)
          + " AND "
          + "END_EFF_DATE "
          + wrapWithOperator(endDate)
          + " AND  "
          + " PLAN_CODE_KEY = '"
          + planCodeKey
          + "' ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { // If record with all the current values does not exist, which means it should be updated.

        // Set up the Plan Code Update.
        qString = "UPDATE LIFEREPL.LR_PLAN_CD_GEN SET ";
        qString += "PRODUCT_TYPE_KEY = "
          //+ wrap(productType)
           + productTypeSecndkey
          + ", "
          + "PLAN_CODE = "
          + wrap(planCode)

          + ", "
          + "AFLIC_CURRENT_IND = "
          + wrap(displayed)
          + ", "
          + "EFF_DATE = "
          + wrap(startDate)
          + ", "
          + "END_EFF_DATE = "
          + wrap(endDate)
          + ", "
          + "UPDATE_ROW_PGMNAME = 'LRTSUPCO', UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = "
          + wrap(userId)
          + " "
          + " WHERE PLAN_CODE_KEY = '"
          + planCodeKey
          + "' ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { // if no record was updated
          try {
            rs2.close();
          } catch (Exception ignore) {}
          try {
            rs.close();
          } catch (Exception ignore) {}
          try {
            stmt2.close();
          } catch (Exception ignore) {}
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Plan Code.  Plan Code record not found.");
        }
      } // If record with all the current values does not exist, which means it should be updated.
      else {
        // do nothing, record does not need to be updated.
      }
    } else { // record to update was not found.
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Plan Code.  Plan Code record not found.");
    }
    // Cleanup...
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }


/**
   * Update the plan code entry for this plan code key.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param companyKey java.lang.String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private void  dbUpdatePlanCodeExtngProdType(

    String planCodeKey,
    String productType,
    String planCode,
    String displayed,
    String startDate,
    String endDate,
    String userId,
    Connection connection)
    throws SQLException {
	logger.debug("dbUpdatePlanCode(String planCodeKey, String productTypeKey, String displayed, String startDate, String endDate, Connection connection");

    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();
    // Read the old record
// check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
    if ((startDate != null) && (!(startDate.trim().equals("")))) {
      try {
        if (startDate.length() > 10) {
          startDate = startDate.substring(0, 10);
        }
        if (LrtsDateHandler.isDateValid(startDate)) {
          startDate = LrtsDateHandler.convertWebDateStringToPersistString(startDate);
        } else {
          throw new SQLException("Date Exception: Start Date : " + startDate + " is not in correct format, or is invalid.");
        }
      } catch (ParseException pe) {}
    }



 // check for proper length of dates, then send through conversion routine to get in proper format for SQL statement.
     if ((endDate != null) && (!(endDate.trim().equals("")))) {
       try {
         if (endDate.length() > 10) {
           endDate = endDate.substring(0, 10);
         }
         if (LrtsDateHandler.isDateValid(endDate)) {
           endDate = LrtsDateHandler.convertWebDateStringToPersistString(endDate);
         } else {
           throw new SQLException("Date Exception: End Date : " + endDate + " is not in correct format, or is invalid.");
         }
       } catch (ParseException pe) {}
 }


   //read the old record
    String qString = "SELECT * FROM LIFEREPL.LR_PLAN_CD_GEN WHERE PLAN_CODE_KEY = '" + planCodeKey + "' ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      // now see if anything has changed
      qString =
        "SELECT PLAN_CODE_KEY FROM LIFEREPL.LR_PLAN_CD_GEN "
          + "WHERE PRODUCT_TYPE_KEY "
          + wrapWithOperator(productType)
          //+ " = " + productTypeSecndkey
          + " AND "
          + "PLAN_CODE "
          + wrapWithOperator(planCode)
          + " AND "
          + "AFLIC_CURRENT_IND "
          + wrapWithOperator(displayed)
          + " AND "
          + "EFF_DATE "
          + wrapWithOperator(startDate)
          + " AND "
          + "END_EFF_DATE "
          + wrapWithOperator(endDate)
          + " AND  "
          + " PLAN_CODE_KEY = '"
          + planCodeKey
          + "' ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { // If record with all the current values does not exist, which means it should be updated.

        // Set up the Plan Code Update.
        qString = "UPDATE LIFEREPL.LR_PLAN_CD_GEN SET ";
        qString += "PRODUCT_TYPE_KEY = "
          + wrap(productType)
         //  + productTypeSecndkey
          + ", "
          + "PLAN_CODE = "
          + wrap(planCode)

          + ", "
          + "AFLIC_CURRENT_IND = "
          + wrap(displayed)
          + ", "
          + "EFF_DATE = "
          + wrap(startDate)
          + ", "
          + "END_EFF_DATE = "
          + wrap(endDate)
          + ", "
          + "UPDATE_ROW_PGMNAME = 'LRTSUPCO', UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = "
          + wrap(userId)
          + " "
          + " WHERE PLAN_CODE_KEY = '"
          + planCodeKey
          + "' ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { // if no record was updated
          try {
            rs2.close();
          } catch (Exception ignore) {}
          try {
            rs.close();
          } catch (Exception ignore) {}
          try {
            stmt2.close();
          } catch (Exception ignore) {}
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Plan Code.  Plan Code record not found.");
        }
      } // If record with all the current values does not exist, which means it should be updated.
      else {
        // do nothing, record does not need to be updated.
      }
    } else { // record to update was not found.
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Plan Code.  Plan Code record not found.");
    }
    // Cleanup...
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }







// ====================== end of plan code
//====================== product type


  /**
   * Checks for an existing policy key and if it does
   * not find one, returns -1.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @return long
   * @param policynumber java.lang.String
   * @param companyKey java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbSelectProductTypeKey(PlanCodeUpdateForm pcu,Connection connection) throws SQLException {
	logger.debug("dbSelectProductTypeKey( Connection connection)");
    Statement stmt = connection.createStatement();
    long returnVal = -1;
    String qString =
      "select max(product_type_key) from liferepl.lr_product_type " ;
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      pcu.setProdTypePrimKey(rs.getLong(1) + 1); //returnVal = rs.getLong(1);
    } else {
      returnVal = -1; // this is a normal case, non-aflic policies are not required to enter a policy number.
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }


  /**
   * Checks for an existing policy key and if it does
   * not find one, returns -1.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @return long
   * @param policynumber java.lang.String
   * @param companyKey java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbSelectPlanCodeKey(PlanCodeUpdateForm pcu,Connection connection) throws SQLException {
	logger.debug("dbSelectPlanCodeKey( Connection connection)");
    Statement stmt = connection.createStatement();
    long returnVal = -1;
    String qString =
      "select max(plan_code_Key) from liferepl.lr_plan_cd_gen " ;
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      pcu.setPlanCodePrimKey(rs.getLong(1) + 1); //returnVal = rs.getLong(1);
    } else {
      returnVal = -1; // this is a normal case, non-aflic policies are not required to enter a policy number.
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }


   /**
   * Checks for an existing policy key and if it does
   * not find one, returns -1.
   * Creation date: (1/2/2003 3:04:54 PM)
   * @return long
   * @param policynumber java.lang.String
   * @param companyKey java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private long dbSelectProductTypeKeyForProdType(ProductTypeUpdateForm pcu,Connection connection) throws SQLException {
	logger.debug("dbSelectProductTypeKey( Connection connection)");
    Statement stmt = connection.createStatement();
    long returnVal = -1;
    String qString =
      "select max(product_type_key) from liferepl.lr_product_type " ;
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    if (rs.next()) {
      pcu.setProdTypePrimKey(rs.getLong(1) + 1); //returnVal = rs.getLong(1);
    } else {
      returnVal = -1; // this is a normal case, non-aflic policies are not required to enter a policy number.
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }
  /**
   * Select routine for PlanCodeUpdateForm object.
   * @param param com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  ProductTypeUpdateForm ProductTypeUpdateFormSelect(ProductTypeUpdateForm param, Connection connection)
    throws SQLException {
    // this is a full select of the insurance company update form.  It is expected that the insurance company key, if found, can be used to fill the
    // form....
	logger.debug("ProductTypeUpdateFormSelect(AdministrationForm param, Connection connection)");
    boolean ok = true;
    // try block
    try {
      // set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      if (param.getProductType() != null) {
   	   // dbSelectProductType(param, connection);
        dbSelectProductType(param, connection);

      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }

//======= update/insert of product type

//enzo
  /**
   * Selects the company information and adds it to the PlanCodeUpdateForm
   * @param icu com.amfam.lrts.form.PlanCodeUpdateForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
void ProductTypeUpdateFormUpdate(ProductTypeUpdateForm param, Connection connection) throws SQLException {
	logger.debug("PlanCodeUpdateForm(PlanCodeUpdateForm param, Connection connection)");
    boolean ok = true;
    String productTypeKey = "";
   // dbSelectProductTypeKey (param,connection);
    try {
      // Set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      dbSelectProductTypeKeyForProdType (param,connection);
      if ((param.getProductTypeKey() == null) || (param.getProductTypeKey().equals(""))) {
        // This is a new product type record. Create it...
        productTypeKey =
          dbInsertNewProductType2(
            param.getProdTypePrimKey(),
            param.getProductType(),
            param.getCurrInd(),
            param.getUserId(),
            connection);
      } else {
        // This is an existing product type record. Update it...
        dbUpdateProductType(
        	param.getProductTypeKey(),
            param.getProductType(),
            param.getCurrInd(),
            param.getUserId(),
            connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
  }

 //newetst
 /*private String dbInsertNewProductType(
    String newProdType,
	String currInd,
	String userId,
	Connection connection)
    throws SQLException {
    LrtsLogger.log(
      this.getClass().getName(),
      LrtsLogger.LOG_DEBUG,
      "dbInsertNewPlanCode(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

    System.out.println("currind1: " + "|" +currInd+"|");
	if ((currInd) ==null) {

    currInd="N";
    System.out.println("currind2: " + currInd);
	}
    String LR_Prod_Type_Cols = "PRODUCT_TYPE_KEY,PRODUCT_TYPE, AFLIC_CURRENT_IND, ";
    LR_Prod_Type_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Prod_Type_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
    String msg="this is the string";
    String qString2= "select max(product_type_key) + 1 from liferepl.lr_product_type where product_type_key < 998 ";
    String qString = "INSERT INTO LIFEREPL.LR_PRODUCT_TYPE (" + LR_Prod_Type_Cols + ") ";
    qString += "VALUES ("      + wrap(newProdType)
      + ", "
      + wrap(currInd)
      + ", "
      + "'LRTSPRTP', SYSDATE, "
      + wrap(userId)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PRODUCT TYPE.");
    }

    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }*/


 //nrtess



   /**
   * Inserts a new product type entry.
   * Creation date: (3/21/2004 9:56:02 aM)
   * @return String
   * @param productType,
   * @param String currInd
   * @param String userId
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewProductType(
    long prodTypePrimKey,
    String productType,
	String currInd,
	String userId,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewProductType2(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

   // System.out.println("currind1: " + "|" +currInd+"|");
	if ((currInd) ==null) {

    currInd="Y";
   // System.out.println("currind2: " + currInd);
	}
	else {
		currInd="Y";
	//	System.out.println("currind3: " + "|" +currInd+"|");

	}
    String LR_Prod_Type_Cols = "PRODUCT_TYPE_KEY,PRODUCT_TYPE, AFLIC_CURRENT_IND, ";
    LR_Prod_Type_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Prod_Type_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
 //   System.out.println("prodKyey: " +"|" +prodTypePrimKey +"|");
    String qString = "INSERT INTO LIFEREPL.LR_PRODUCT_TYPE (" + LR_Prod_Type_Cols + ") ";
    qString += "VALUES ("
      + prodTypePrimKey
      + ", "
      + wrap(productType)
      + ", "
      + wrap(currInd)
      + ", "
      + "'LRTSPRTP', SYSDATE, "
      + wrap(userId)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PRODUCT TYPE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }



   /**
   * Inserts a new product type entry.
   * Creation date: (3/21/2004 9:56:02 aM)
   * @return String
   * @param productType,
   * @param String currInd
   * @param String userId
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  private String dbInsertNewProductType2(
    long prodTypePrimKey,
    String productType,
	String currInd,
	String userId,
	Connection connection)
    throws SQLException {
	logger.debug("dbInsertNewProductType2(String productTypeKey,String planCode,String displayed,String startDate,String endDate(),	Connection connection)");

  //  System.out.println("currind1: " + "|" +currInd+"|");
	if ((currInd) ==null) {

    currInd="N";
    //System.out.println("currind2: " + currInd);
	}
	else {
		currInd="N";
	//	System.out.println("currind3: " + "|" +currInd+"|");

	}
    String LR_Prod_Type_Cols = "PRODUCT_TYPE_KEY,PRODUCT_TYPE, AFLIC_CURRENT_IND, ";
    LR_Prod_Type_Cols += "CREATE_ROW_PGMNAME, CREATE_ROW_TS, CREATE_ROW_USERID, ";
    LR_Prod_Type_Cols += "UPDATE_ROW_PGMNAME, UPDATE_ROW_TS, UPDATE_ROW_USERID";
    Statement stmt = connection.createStatement();
    String returnVal = "";
   // System.out.println("prodKyey: " +"|" +prodTypePrimKey +"|");
    String qString = "INSERT INTO LIFEREPL.LR_PRODUCT_TYPE (" + LR_Prod_Type_Cols + ") ";
    qString += "VALUES ("
      + prodTypePrimKey
      + ", "
      + wrap(productType)
      + ", "
      + wrap(currInd)
      + ", "
      + "'LRTSPRTP', SYSDATE, "
      + wrap(userId)
      + ", "
      + "null, null, null)";
    logger.debug(qString);

    if (stmt.executeUpdate(qString) == 0) { // if no inserts happened
      try {
      	    logger.debug(qString);
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No insert performed on PRODUCT TYPE.");
    }

    try {
      stmt.close();
    } catch (Exception ignore) {}
    return returnVal;
  }


/**
   * Update the plan code entry for this plan code key.
   * Creation date: (1/2/2003 3:01:02 PM)
   * @param companyKey java.lang.String
   * @param companyName java.lang.String
   * @param address1 java.lang.String
   * @param address2 java.lang.String
   * @param city java.lang.String
   * @param state java.lang.String
   * @param zip java.lang.String
   * @param country java.lang.String
   * @param displayed java.lang.String
   * @param userid java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */

//enzo2
  private void dbUpdateProductType(
    String productTypeKey,
	String productType,
	String currInd,
	String userId,
    Connection connection)
    throws SQLException {
	logger.debug("dbUpdateProductType(String productTypeKey, String productType, String currInd, String userId, Connection connection");

    Statement stmt = connection.createStatement();
    Statement stmt2 = connection.createStatement();

   //read the old record
    String qString = "SELECT * FROM LIFEREPL.LR_PRODUCT_TYPE WHERE PRODUCT_TYPE_KEY= '" + productTypeKey + "' ";
    logger.debug(qString);
    ResultSet rs = stmt.executeQuery(qString);
    ResultSet rs2 = null;
    if (rs.next()) {
      // now see if anything has changed
      qString =
        "SELECT PRODUCT_TYPE_KEY FROM LIFEREPL.LR_PRODUCT_TYPE "
          + "WHERE PRODUCT_TYPE "
          + wrapWithOperator(productType)
          + " AND "
          + "AFLIC_CURRENT_IND "
          + wrapWithOperator(currInd)
          + " AND  "
          + " PRODUCT_TYPE_KEY = '"
          + productTypeKey
          + "' ";
      logger.debug(qString);
      rs2 = stmt2.executeQuery(qString);
      if (!(rs2.next())) { // If record with all the current values does not exist, which means it should be updated.

        // Set up the Plan Code Update.
        qString = "UPDATE LIFEREPL.LR_PRODUCT_TYPE SET ";
        qString += "PRODUCT_TYPE = "
          + wrap(productType)
          + ", "
          + "AFLIC_CURRENT_IND = "
          + wrap(currInd)
          + ", "
          + "UPDATE_ROW_PGMNAME = 'LRTSUPCO', UPDATE_ROW_TS = SYSDATE, UPDATE_ROW_USERID = "
          + wrap(userId)
          + " "
          + " WHERE PRODUCT_TYPE_KEY = '"
          + productTypeKey
          + "' ";
        logger.debug(qString);
        if (stmt.executeUpdate(qString) == 0) { // if no record was updated
          try {
            rs2.close();
          } catch (Exception ignore) {}
          try {
            rs.close();
          } catch (Exception ignore) {}
          try {
            stmt2.close();
          } catch (Exception ignore) {}
          try {
            stmt.close();
          } catch (Exception ignore) {}
          throw new SQLException("No update performed on Product Type.  Product Type record not found.");
        }
      } // If record with all the current values does not exist, which means it should be updated.
      else {
        // do nothing, record does not need to be updated.
      }
    } else { // record to update was not found.
      try {
        rs2.close();
      } catch (Exception ignore) {}
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt2.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
      throw new SQLException("No update performed on Product Type.  Product Type record not found.");
    }
    // Cleanup...
    try {
      rs2.close();
    } catch (Exception ignore) {}
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt2.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
  }




// ======================== end of product type

  /**
   * Add routine for the MainEntryForm object.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @param param com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  void mainEntryFormAdd(MainEntryForm param, Connection connection) throws SQLException {
    logger.debug("mainEntryFormAdd(MainEntryForm param, Connection connection)");
    boolean ok = true;
    //try block
    try {
      //set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      String replacedInsuranceKey = "";
      String replacingInsuranceKey = "";
      long replacedPolicyKey = 0L;
      long replacingPolicyKey = 0L;
      long reportingKey = 0L;
      long policyKeyVal = 0;
      // new insurance company on replaced?
      if (param.getReplacedCompanyLookup().trim().equals("")) { // insert
        replacedInsuranceKey =
          dbInsertNewCompany(
            param.getReplacedCompanyName(),
            param.getReplacedAddress1(),
            param.getReplacedAddress2(),
            param.getReplacedCity(),
            param.getReplacedState(),
            param.getReplacedZipCode(),
            param.getReplacedCountry(),
            LrtsConstants.YES,
            param.getUserId(),
            connection);
      } else {
        replacedInsuranceKey = param.getReplacedCompanyLookup();
      }
      // new insurance company on replacing?
      if (param.getReplacingCompanyLookup().trim().equals("")) { // insert
        replacingInsuranceKey =
          dbInsertNewCompany(
            param.getReplacingCompanyName(),
            param.getReplacingAddress1(),
            param.getReplacingAddress2(),
            param.getReplacingCity(),
            param.getReplacingState(),
            param.getReplacingZipCode(),
            param.getReplacingCountry(),
            LrtsConstants.YES,
            param.getUserId(),
            connection);
      } else {
        replacingInsuranceKey = param.getReplacingCompanyLookup();
      }
      replacedPolicyKey =
        dbInsertPolicy(
          param.getReplacedPlanCode(),
          param.getReplacedContractState(),
          param.getReplacedProduct(),
          replacedInsuranceKey,
          param.getReplacedPolicyNumber(),
          param.getReplacedLastName(),
          param.getReplacedFirstName(),
          param.getReplacedMiddleInitial(),
          param.getReplacedSuffix(),
          param.getReplacedAgentCode(),
          param.getReplacedDistrictCode(),
          param.getUserId(),
          connection);
      replacingPolicyKey =
        dbInsertPolicy(
          param.getReplacingPlanCode(),
          param.getReplacingContractState(),
          param.getReplacingProduct(),
          replacingInsuranceKey,
          param.getReplacingPolicyNumber(),
          param.getReplacingLastName(),
          param.getReplacingFirstName(),
          param.getReplacingMiddleInitial(),
          param.getReplacingSuffix(),
          param.getReplacingAgentCode(),
          param.getReplacingDistrictCode(),
          param.getUserId(),
          connection);
      //insert reporting instance
      reportingKey = dbInsertReport(param.getUserId(), connection);
      //insert reporting_ind
      dbInsertReportingInd(reportingKey, param, connection);
      //insert to lr_policy_actual
      dbInsertPolicyActual(replacingPolicyKey, replacedPolicyKey, param.getUserId(), connection);
      //insert policy_report
      dbInsertPolicyReport(reportingKey, replacingPolicyKey, replacedPolicyKey, param.getUserId(), connection);
      //insert transaction instance
      dbInsertTransaction(reportingKey, replacingPolicyKey, replacedPolicyKey, param.getUserId(), connection);
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
  }
  /**
   * Select routine for MainEntryForm object.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @param param com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  MainEntryForm mainEntryFormSelect(MainEntryForm param, Connection connection) throws SQLException {
    // this is a full select of the main entry form.  It is expected that the policy keys, if found, can be used to fill the
    // entry form....
    logger.debug("mainEntryFormSelect(MainEntryForm param, Connection connection)");
    boolean ok = true;
    // try block
    try {
      // set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      int replacedInsuranceKey = 0;
      int replacingInsuranceKey = 0;
      long replacedPolicyKey = 0L;
      long replacingPolicyKey = 0L;
      long reportingKey = 0L;
      long policyKeyVal = 0L;
      if ((param.getReplacingPolicyKey() != null) && (param.getReplacedPolicyKey() != null)) {
        replacingPolicyKey = Long.parseLong(param.getReplacingPolicyKey());
        replacedPolicyKey = Long.parseLong(param.getReplacedPolicyKey());
        dbSelectPolicies(param, replacingPolicyKey, replacedPolicyKey, connection);
        dbSelectCompanies(param, connection);
        reportingKey = dbSelectPolicyReport(replacingPolicyKey, replacedPolicyKey, connection);
        param.setComment(dbSelectComment(reportingKey, connection));
        dbSelectReportingInd(reportingKey, param, connection);
        dbSelectTransaction(reportingKey, replacingPolicyKey, replacedPolicyKey, param, connection);
      }
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }
  /**
   * Selects the insurance company info and loads
   * it in the main entry form.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @param param com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  MainEntryForm mainEntryFormSelectInsuranceCo(MainEntryForm param, Connection connection) throws SQLException {
    //modified select method to only look up the insurance company and load only that data into the main entry form.
	logger.debug("mainEntryFormSelectInsuranceCo(MainEntryForm param, Connection connection)");
    boolean ok = true;
    //try block
    try {
      //set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      int replacedInsuranceKey = 0;
      int replacingInsuranceKey = 0;
      long replacedPolicyKey = 0L;
      long replacingPolicyKey = 0L;
      long reportingKey = 0L;
      long policyKeyVal = 0;
      dbSelectCompanies(param, connection);
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }
  /**
   * Method fills entry form with what is found for a
   * given policy number.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @param param com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  MainEntryForm mainEntryFormSelectPolicy(MainEntryForm param, Connection connection) throws SQLException {
	logger.debug("mainEntryFormSelectPolicy(MainEntryForm param, Connection connection)");
    boolean ok = true;
    //try block
    try {
      //set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      int replacedInsuranceKey = 0;
      int replacingInsuranceKey = 0;
      long replacedPolicyKey = 0L;
      long replacingPolicyKey = 0L;
      long reportingKey = 0L;
      long policyKeyVal = 0;
      if (param.getReplacingPolicyNumber() != null) {
        replacingPolicyKey =
          dbSelectPolicyKey(param.getReplacingPolicyNumber(), Integer.toString(LrtsProperties.getAflicNumber()), connection);
        if (replacingPolicyKey > -1) {
          param.setReplacingCompanyLookup(Integer.toString(LrtsProperties.getAflicNumber()));
        }
      } else {
        replacingPolicyKey = -1;
      }
      if (param.getReplacedPolicyNumber() != null) {
        replacedPolicyKey =
          dbSelectPolicyKey(param.getReplacedPolicyNumber(), Integer.toString(LrtsProperties.getAflicNumber()), connection);
        if (replacedPolicyKey > -1) {
          param.setReplacedCompanyLookup(Integer.toString(LrtsProperties.getAflicNumber()));
        }
      } else {
        replacedPolicyKey = -1;
      }
      dbSelectCompanies(param, connection);
      dbSelectPolicies(param, replacingPolicyKey, replacedPolicyKey, connection);
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
    return param;
  }
  public void adminAuthorizationFormDelete(AdminAuthorizationForm parmForm) throws SQLException {
  	Connection theConnection = PersistenceManagerImpl.getLrtsConnection() ;
  	try {
	    adminAuthorizationFormDelete(parmForm, theConnection);
  	} finally {
	    PersistenceManagerImpl.returnLrtsConnection(theConnection) ;
  	}
  }
  public void adminContractStateUpdate(AdminStateForm parmForm) throws SQLException {
  	Connection theConnection = PersistenceManagerImpl.getLrtsConnection() ;
  	try {
	    adminContractStateUpdate(parmForm, theConnection);
  	} finally {
	    PersistenceManagerImpl.returnLrtsConnection(theConnection) ;
  	}
  }
  void adminAuthorizationFormDelete(AdminAuthorizationForm parmForm, Connection parmConnection) throws SQLException {
    if (parmForm == null)
      return;
    AmFamUser theUsers[] = parmForm.getExistingUser();
    if (theUsers != null) {
      int numUsers = theUsers.length;
      boolean didDelete = false ;
      try {
	      for (int i = 0; i < numUsers; i++) {
	        if ((theUsers[i] != null) && (theUsers[i].isSelected())) {
	          dbDeleteAuthorizedUser(theUsers[i].getUserId(), parmConnection);
	          didDelete=true ;
	        }
	      }
      } finally {
      	if (didDelete) parmForm.setExistingUser(null) ;
      }
    }
  }
  void adminContractStateUpdate(AdminStateForm parmForm, Connection parmConnection) throws SQLException {
    if (parmForm == null)
      return;
	ContractState theUpdateState = parmForm.getUpdateState() ;
    if (theUpdateState == null) throw new SQLException("PWorker.adminContractStateUpdate(), null updateState") ;
      boolean didUpdate=false ;
      try {
	      dbUpdateContractState(theUpdateState.getCode(),theUpdateState.getName(),theUpdateState.isAFLIC(),parmConnection) ;
	      didUpdate=true ;
      } finally {
      	if (didUpdate) {
      		parmForm.setUpdateState(null) ;
      		parmForm.setCurrentContractState(null) ;
      	}
      }
  }
  public void adminAuthorizationFormInsert(AdminAuthorizationForm parmForm) throws SQLException {
  	Connection theConnection = PersistenceManagerImpl.getLrtsConnection() ;
  	try {
	    adminAuthorizationFormInsert(parmForm, theConnection);
  	} finally {
	    PersistenceManagerImpl.returnLrtsConnection(theConnection) ;
  	}
  }
  void adminAuthorizationFormInsert(AdminAuthorizationForm parmForm, Connection parmConnection) throws SQLException {
    if (parmForm == null)
      return;
    AmFamUser theNewUser = parmForm.getNewUser();
    if (theNewUser != null) {
      dbInsertAuthorizedUser(theNewUser.getUserId(), theNewUser.getName(), parmConnection);
      parmForm.setExistingUser(null) ;
    }
  }
  public void adminAuthorizationFormSelect(AdminAuthorizationForm parmForm) throws SQLException {
  	Connection theConnection = PersistenceManagerImpl.getLrtsConnection() ;
  	try {
	    adminAuthorizationFormSelect(parmForm, theConnection);
  	} finally {
	    PersistenceManagerImpl.returnLrtsConnection(theConnection) ;
  	}

  }
  public void adminStateFormSelect(AdminStateForm parmForm) throws SQLException {
  	Connection theConnection = PersistenceManagerImpl.getLrtsConnection() ;
  	try {
	    adminStateFormSelect(parmForm, theConnection);
  	} finally {
	    PersistenceManagerImpl.returnLrtsConnection(theConnection) ;
  	}

  }
    void adminStateFormSelect(AdminStateForm parmForm, Connection parmConnection) throws SQLException {
      if (parmForm == null)
            return;
      ResultSet theResults = dbSelectContractState(parmConnection);
      if (theResults == null)
            return;
        ArrayList theStates = new ArrayList() ;
        ContractState lastItem = null ;
        while (theResults.next()) {
          ContractState nextItem = new ContractState() ;
          nextItem.setCode(theResults.getString(1));
          nextItem.setName(theResults.getString(2));
          boolean isAflic = theResults.getString(3).trim().toUpperCase().equals("Y") ;
          nextItem.setAFLIC(isAflic) ;
          if (nextItem.getCode().equals("N/A")) {
          	lastItem = nextItem ;
          } else {
	          theStates.add(nextItem);
          }
        }
        if (lastItem != null) theStates.add(lastItem) ;
        parmForm.setCurrentContractState((ContractState[]) theStates.toArray(new ContractState[theStates.size()])) ;
        closeResultSet(theResults);
      }
  /**
   * Do update on data in main entry form.
   * Creation date: (12/13/2002 1:25:42 PM)
   * @param param com.amfam.lrts.form.MainEntryForm
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException The exception description.
   */
  void mainEntryFormUpdate(MainEntryForm param, Connection connection) throws SQLException {
    logger.debug("mainEntryFormUpdate(MainEntryForm param, Connection connection)");
    boolean ok = true;
    try {
      // Set up connection and values
      connection.setAutoCommit(false);
      String qString = "";
      String replacedInsuranceKey = "";
      String replacingInsuranceKey = "";
      long replacedPolicyKey = 0L;
      long replacingPolicyKey = 0L;
      long reportingKey = 0L;
      long policyKeyVal = 0L;
      replacingPolicyKey = Long.parseLong(param.getReplacingPolicyKey());
      replacedPolicyKey = Long.parseLong(param.getReplacedPolicyKey());
      if ((param.getReplacingCompanyLookup() == null) || (param.getReplacingCompanyLookup().trim().equals(""))) {
        if ((param.getReplacingInsuranceCoKey() == null) || (param.getReplacingInsuranceCoKey().equals(""))) {
          replacingInsuranceKey =
            dbInsertNewCompany(
              param.getReplacingCompanyName(),
              param.getReplacingAddress1(),
              param.getReplacingAddress2(),
              param.getReplacingCity(),
              param.getReplacingState(),
              param.getReplacingZipCode(),
              param.getReplacingCountry(),
              LrtsConstants.YES,
              param.getUserId(),
              connection);
        } else {
          replacingInsuranceKey = param.getReplacingInsuranceCoKey();
          dbUpdateCompany(
            replacingInsuranceKey,
            param.getReplacingCompanyName(),
            param.getReplacingAddress1(),
            param.getReplacingAddress2(),
            param.getReplacingCity(),
            param.getReplacingState(),
            param.getReplacingZipCode(),
            param.getReplacingCountry(),
            LrtsConstants.YES,
            param.getUserId(),
            connection);
        }
      } else {
        replacingInsuranceKey = param.getReplacingCompanyLookup();
      }
      dbUpdatePolicy(
        replacingPolicyKey,
        param.getReplacingPlanCode(),
        param.getReplacingContractState(),
        param.getReplacingProduct(),
        replacingInsuranceKey,
        param.getReplacingPolicyNumber(),
        param.getReplacingLastName(),
        param.getReplacingFirstName(),
        param.getReplacingMiddleInitial(),
        param.getReplacingSuffix(),
        param.getReplacingAgentCode(),
        param.getReplacingDistrictCode(),
        param.getUserId(),
        connection);
      if ((param.getReplacedCompanyLookup() == null) || (param.getReplacedCompanyLookup().trim().equals(""))) {
        if ((param.getReplacedInsuranceCoKey() == null) || (param.getReplacedInsuranceCoKey().equals(""))) {
          replacedInsuranceKey =
            dbInsertNewCompany(
              param.getReplacedCompanyName(),
              param.getReplacedAddress1(),
              param.getReplacedAddress2(),
              param.getReplacedCity(),
              param.getReplacedState(),
              param.getReplacedZipCode(),
              param.getReplacedCountry(),
              LrtsConstants.YES,
              param.getUserId(),
              connection);
        } else {
          replacedInsuranceKey = param.getReplacedInsuranceCoKey();
          dbUpdateCompany(
            replacedInsuranceKey,
            param.getReplacedCompanyName(),
            param.getReplacedAddress1(),
            param.getReplacedAddress2(),
            param.getReplacedCity(),
            param.getReplacedState(),
            param.getReplacedZipCode(),
            param.getReplacedCountry(),
            LrtsConstants.YES,
            param.getUserId(),
            connection);
        }
      } else {
        replacedInsuranceKey = param.getReplacedCompanyLookup();
      }
      dbUpdatePolicy(
        replacedPolicyKey,
        param.getReplacedPlanCode(),
        param.getReplacedContractState(),
        param.getReplacedProduct(),
        replacedInsuranceKey,
        param.getReplacedPolicyNumber(),
        param.getReplacedLastName(),
        param.getReplacedFirstName(),
        param.getReplacedMiddleInitial(),
        param.getReplacedSuffix(),
        param.getReplacedAgentCode(),
        param.getReplacedDistrictCode(),
        param.getUserId(),
        connection);
      // Get reporting key
      reportingKey = dbSelectPolicyReport(replacingPolicyKey, replacedPolicyKey, connection);
      //update reporting instance
      //commented out because none of this data will change....
      //dbUpdateReport(reportingKey, param.getUserId(), connection);
      // update reporting_ind
      dbUpdateReportingInd(reportingKey, param, connection);
      // update transaction instance
      dbUpdateTransaction(
        reportingKey,
        replacingPolicyKey,
        replacedPolicyKey,
        param.getSurrenderDate(),
        param.getTransactionDate(),
        param.getUserId(),
        connection);
      // update comment
      //commented out on 2/27/03.  May need to wipe out an existing comment.  This code would prevent that from happening - C. Pauer TeamSoft INC.
      //		if ((param.getComment()!=null) && !(param.getComment().trim().equals(""))) {
      dbInsertComment(reportingKey, param.getComment(), param.getUserId(), connection);
      //		}
    } catch (SQLException sqle) {
      ok = false;
      connection.rollback();
      connection.setAutoCommit(true);
      logger.error("Exception: " + sqle);
      throw sqle;
    } finally {
      if (ok) {
        connection.commit();
        connection.setAutoCommit(true);
      }
    }
  }
  /**
   * Checks to see if policy number for a given insurance company already exists.
   * Creation date: (1/2/2003 3:21:10 PM)
   * @return boolean
   * @param policyNumber java.lang.String
   * @param company java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  private boolean policyExists(String policyNumber, String company, Connection connection) {
	logger.debug("policyExists(String policyNumber, String company, Connection connection)");
    boolean returnVal = false;
    try {
      Statement stmt = connection.createStatement();
      String qString =
        "SELECT POLICY_KEY FROM LIFEREPL.LR_POLICY_GEN WHERE POLICY_NUMBER = '" + policyNumber + "' AND " + "INSURANCE_CO_KEY = " + company;
      logger.debug(qString);
      ResultSet rs = stmt.executeQuery(qString);
      if (rs.next()) {
        returnVal = true;
      }
      try {
        rs.close();
      } catch (Exception ignore) {}
      try {
        stmt.close();
      } catch (Exception ignore) {}
    } catch (SQLException sqle) {
      logger.error("Exception: " + sqle);
    }
    return returnVal;
  }
  /**
   * Method does selections on read only data.  This data is used
   * to populate drop down or selection lists.
   * Creation date: (12/12/2002 1:33:38 PM)
   * @return MatchedStringListCollection
   * @param sortOrder java.lang.String
   * @param fetchType java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  MatchedStringListCollection select(String sortOrder, String fetchType, Connection connection) throws SQLException {
	logger.debug("select(String sortOrder, String fetchType, Connection connection)");
    MatchedStringListCollection mslc = new MatchedStringListCollection();
    //the first few fetches do not originate from a database.  They are hardcode here.  Just a judgement that these
    //particular values will not change.
    if (fetchType.equalsIgnoreCase(PersistenceManager.FETCH_TYPE_YES_NO)) {
      mslc.addList(Arrays.asList(new String[] { "Y", "N" }));
      mslc.addList(Arrays.asList(new String[] { "YES", "NO" }));
      return mslc;
    }
    if (fetchType.equalsIgnoreCase(PersistenceManager.FETCH_TYPE_REPLACEMENT_TYPE)) {
      mslc.addList(Arrays.asList(new String[] { "E", "I", "R" }));
      mslc.addList(Arrays.asList(new String[] { "EXTERNAL", "INTERNAL", "REPLACED" }));
      return mslc;
    }

    if (fetchType.equalsIgnoreCase(PersistenceManager.FETCH_TYPE_REPLACED_REPLACING_NODISP)) {
      mslc.addList(Arrays.asList(new String[] { "Y", "N", "R" }));
      mslc.addList(Arrays.asList(new String[] { "REPLACING", "REPLACED", "DO NOT DISPLAY" }));
      return mslc;
    }


    if (fetchType.equalsIgnoreCase(PersistenceManager.FETCH_TYPE_AFLIC_CHOICE)) {
      mslc.addList(Arrays.asList(new String[] { "Y" }));
      mslc.addList(Arrays.asList(new String[] { "AFLIC" }));
      return mslc;
    }
    if (fetchType.equalsIgnoreCase(PersistenceManager.FETCH_TYPE_AFLIC_OTHER_CHOICE)) {
      mslc.addList(Arrays.asList(new String[] { "Y", "N" }));
      mslc.addList(Arrays.asList(new String[] { "AFLIC", "OTHER" }));
      return mslc;
    }
    Statement stmt = connection.createStatement();
    //from here on out we start putting pieces of
    //SQL Statements,  based on the fetchtype and sort order constants that were
    //sent as parameters to this call, into a hashtable.
    //init values
    Hashtable ht = new Hashtable();
    ht.put(PersistenceManager.FETCH_TYPE_STATE + "colnames", "CONTRACT_STATE_ID, STATE_NAME");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_AFLIC + "colnames", "CONTRACT_STATE_ID, STATE_NAME");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_INSCO + "colnames", "STATE_CODE, STATE_NAME");
    ht.put(PersistenceManager.FETCH_TYPE_CO + "colnames", "INSURANCE_CO_KEY, COMPANY_NAME");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_AFLIC + "colnames", "PRODUCT_TYPE_KEY, PRODUCT_TYPE");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_NO_AFLIC + "colnames", "PRODUCT_TYPE_KEY, PRODUCT_TYPE");
    //added select product type select all - EGI001
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_ALL + "colnames", "PRODUCT_TYPE_KEY, PRODUCT_TYPE");

    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_AFLIC + "colnames", "PLAN_CODE_KEY, PLAN_CODE");
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_NO_AFLIC + "colnames", "PLAN_CODE_KEY, PLAN_CODE");
    ht.put(PersistenceManager.FETCH_TYPE_STATE, "LIFEREPL.LR_CONTRACT_STATE");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_AFLIC, "LIFEREPL.LR_CONTRACT_STATE");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_INSCO, "LIFEREPL.LR_CONTRACT_STATE");
    ht.put(PersistenceManager.FETCH_TYPE_CO, "LIFEREPL.LR_INSURANCE_COMPANY");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_AFLIC, "LIFEREPL.LR_PRODUCT_TYPE");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_NO_AFLIC, "LIFEREPL.LR_PRODUCT_TYPE");
    //added select product type select all - EGI001
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_ALL, "LIFEREPL.LR_PRODUCT_TYPE");
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_AFLIC, "LIFEREPL.LR_PLAN_CD_GEN");
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_NO_AFLIC, "LIFEREPL.LR_PLAN_CD_GEN");
    // changed 2/23/03 to sort by state_name  - C.Pauer  TeamSoft Inc.
    ht.put(PersistenceManager.FETCH_TYPE_STATE + "order", "ORDER BY STATE_NAME " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_STATE_AFLIC + "order", "ORDER BY STATE_NAME " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_STATE_INSCO + "order", "ORDER BY STATE_NAME " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_CO + "order", "ORDER BY COMPANY_NAME " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_AFLIC + "order", "ORDER BY PRODUCT_TYPE " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_NO_AFLIC + "order", "ORDER BY PRODUCT_TYPE " + sortOrder);
    //added select product type select all - EGI001
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_ALL  + "order", "ORDER BY PRODUCT_TYPE " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_AFLIC + "order", "ORDER BY PLAN_CODE " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_NO_AFLIC + "order", "ORDER BY PLAN_CODE " + sortOrder);
    ht.put(PersistenceManager.FETCH_TYPE_STATE + "where", "");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_AFLIC + "where", "WHERE AFLIC_STATE_IND = 'Y'");
    ht.put(PersistenceManager.FETCH_TYPE_STATE_INSCO + "where", "");
    ht.put(PersistenceManager.FETCH_TYPE_CO + "where", "WHERE COMPANY_SHOW_IN_LIST = 'Y'");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_AFLIC + "where", "WHERE AFLIC_CURRENT_IND = 'Y'");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_NO_AFLIC + "where", "WHERE AFLIC_CURRENT_IND <> 'Y'");
    ht.put(PersistenceManager.FETCH_TYPE_PRODUCT_ALL + "where", "WHERE PRODUCT_TYPE_KEY <> '998' and PRODUCT_TYPE_KEY <> '999' ");
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_AFLIC + "where", "WHERE AFLIC_CURRENT_IND = 'Y'");
    ht.put(PersistenceManager.FETCH_TYPE_PLAN_CODE_NO_AFLIC + "where", "WHERE AFLIC_CURRENT_IND = 'N'");
    //define parts of SQL request
    String operation = "SELECT";
    //Construct SQL string
    String stmtString = "";
    //set operation
    stmtString += operation + " ";
    //columnnames
    stmtString += ht.get(fetchType + "colnames") + " ";
    stmtString += "FROM ";
    //determine tablename
    stmtString += ht.get(fetchType) + " ";
    //whereclause
    stmtString += ht.get(fetchType + "where") + " ";
    //order by
    stmtString += ht.get(fetchType + "order");
    logger.debug(stmtString);
    ResultSet rs = stmt.executeQuery(stmtString);
    ResultSetMetaData rsmd = rs.getMetaData();
    ArrayList[] ala = new ArrayList[rsmd.getColumnCount()]; //declare as many arraylists as there are columns
    for (int i = 0; i < rsmd.getColumnCount(); i++) { //for each column
      ala[i] = new ArrayList();
    }
    while (rs.next()) { //for each record
      for (int i = 0; i < rsmd.getColumnCount(); i++) { //for each column
        ala[i].add(rs.getString(rsmd.getColumnName(i + 1))); //add that record's column entry in the appropriate arraylist
      }
    }
    //in the end, the matched string list collection holds a list for each columns that the fetchtype indicated, with as
    //many entries in the list as there were records returned.
    for (int i = 0; i < rsmd.getColumnCount(); i++) { //for each column
      mslc.addList(ala[i]);
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return mslc;
  }
  /**
   * A special static fetch for population of the update selection screen
   * Creation date: (12/12/2002 1:33:38 PM)
   * @return MatchedStringListCollection
   * @param contractNumber java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up
   *	 to the business layer.
   */
  MatchedStringListCollection selectPolicyList(String contractNumber, Connection connection) throws SQLException {
    //this list building is kept separate from the other list fetches in select because the SQL here is much more
    //complicated....The values that need to be sent back in the collection come from 3 different tables, with more than one select
    //possible on a table....
    logger.debug("selectPolicyList(String contractNumber, Connection connection)");
    MatchedStringListCollection mslc = new MatchedStringListCollection();
    Statement stmt = connection.createStatement();
    String stmtString = "";
    ArrayList[] ala = new ArrayList[7];
    for (int i = 0; i < 7; i++) {
      ala[i] = new ArrayList();
    }
    long selectedPolicyKey = 0L;
    // Construct SQL string
    //The customer indicates that they want to update certain aflic policy...
    //looking at the Where clause....you can see the policy number is what is requested, the co_key is
    //assumed to be AFLIC.  The Reporting Indicator has to be true.  We sort by the creation time of the
    //policy actual table (this was when the record first appeared in the database.)
    //Taking a look at what is selected:
    //we select the columns if the policy entries, either replaced or replacing, equal the policy key on the policy_gen...
    //then grab the reporting key based on the replacing/replaced keys, and use that to get the report indicators....
    stmtString =
      "SELECT LR_POLICY_ACTUAL.REPLACING_POLICY_KEY, LR_POLICY_ACTUAL.REPLACED_POLICY_KEY, LR_POLICY_ACTUAL.CREATE_ROW_TS FROM "
        + "LIFEREPL.LR_POLICY_ACTUAL "
        + "	JOIN LIFEREPL.LR_POLICY_GEN ON (REPLACING_POLICY_KEY = POLICY_KEY) OR (REPLACED_POLICY_KEY = POLICY_KEY) "
        + "	JOIN LIFEREPL.LR_POLICY_REPORT ON (LR_POLICY_ACTUAL.REPLACING_POLICY_KEY = LR_POLICY_REPORT.REPLACING_POLICY_KEY AND "
        + "	LR_POLICY_ACTUAL.REPLACED_POLICY_KEY = LR_POLICY_REPORT.REPLACED_POLICY_KEY) "
        + "	LEFT JOIN LIFEREPL.LR_REPORTING_IND ON LR_POLICY_REPORT.REPORTING_KEY = LR_REPORTING_IND.REPORTING_KEY "
        + "	JOIN LIFEREPL.LR_INDICATOR ON LR_REPORTING_IND.IND_KEY = LR_INDICATOR.IND_KEY "
        + "	WHERE POLICY_NUMBER='"
        + contractNumber
        + "' AND INSURANCE_CO_KEY = "
        + LrtsProperties.getAflicNumber()
        + " AND "
        + "	LR_INDICATOR.IND_CD = 'REPORTING INDICATOR' AND "
        + "	LR_REPORTING_IND.IND_VALUE = 'Y' "
        + "	ORDER BY LR_POLICY_ACTUAL.CREATE_ROW_TS DESC ";
    //this gets us all the replacing, replaced and timestamps that meet our above criteria.  These are three of the columns we need
    //for the update selection...more selects below...
    logger.debug(stmtString);
    ResultSet rs = stmt.executeQuery(stmtString);
    boolean someRecords = false;
    //start building the lists that will be added to the MSLC.
    while (rs.next()) { //for each record
      someRecords = true;
      ala[0].add(unwrap(rs.getString("REPLACING_POLICY_KEY"))); //add that record's column entry in the appropriate arraylist
      ala[1].add(unwrap(rs.getString("REPLACED_POLICY_KEY"))); //add that record's column entry in the appropriate arraylist
      ala[6].add(LrtsDateHandler.convertDateToOutputString(rs.getDate("CREATE_ROW_TS")));
      //add that record's column entry in the appropriate arraylist
    }
    if (!someRecords) {
      return null; //if nothing found, presentation wants null back...
    }
    for (int i = 0; i < 7; i++) { //for each column
      mslc.addList(ala[i]);
    }
    //ok, now select all the policy info for the replacing policy, for each record.
    for (int i = 0; i < ala[0].size(); i++) {
      stmtString = "SELECT POLICY_NUMBER FROM LIFEREPL.LR_POLICY_GEN " + " WHERE POLICY_KEY = '" + ala[0].get(i) + "' ";
      logger.debug(stmtString);
      rs = stmt.executeQuery(stmtString);
      //put that info in column 2.
      while (rs.next()) { //for each record
        ala[2].add(unwrap(rs.getString("POLICY_NUMBER"))); //add that record's column entry in the appropriate arraylist
      }
    }
    //now select the policy info for the replaced policy
    for (int i = 0; i < ala[1].size(); i++) {
      stmtString = "SELECT POLICY_NUMBER FROM LIFEREPL.LR_POLICY_GEN " + " WHERE POLICY_KEY = '" + ala[1].get(i) + "' ";
      logger.debug(stmtString);
      rs = stmt.executeQuery(stmtString);
      //put that in column 4
      while (rs.next()) { //for each record
        ala[4].add(unwrap(rs.getString("POLICY_NUMBER"))); //add that record's column entry in the appropriate arraylist
      }
    }
    // now we need to get the company name from the company table, based on the replacing policy
    for (int i = 0; i < ala[0].size(); i++) {
      stmtString =
        "SELECT COMPANY_NAME FROM LIFEREPL.LR_POLICY_GEN, LIFEREPL.LR_INSURANCE_COMPANY "
          + " WHERE POLICY_KEY = '"
          + ala[0].get(i)
          + "' AND LR_POLICY_GEN.INSURANCE_CO_KEY = LR_INSURANCE_COMPANY.INSURANCE_CO_KEY";
      logger.debug(stmtString);
      rs = stmt.executeQuery(stmtString);
      //the replacing company name goes in column 3
      while (rs.next()) { //for each record
        ala[3].add(unwrap(rs.getString("COMPANY_NAME"))); //add that record's column entry in the appropriate arraylist
      }
    }
    // now we need to get the company name from the company table, based on the replaced policy
    for (int i = 0; i < ala[1].size(); i++) {
      stmtString =
        "SELECT COMPANY_NAME FROM LIFEREPL.LR_POLICY_GEN, LIFEREPL.LR_INSURANCE_COMPANY "
          + " WHERE POLICY_KEY = '"
          + ala[1].get(i)
          + "' AND LR_POLICY_GEN.INSURANCE_CO_KEY = LR_INSURANCE_COMPANY.INSURANCE_CO_KEY";
      logger.debug(stmtString);
      rs = stmt.executeQuery(stmtString);
      //the replaced company name goes in column 5
      while (rs.next()) { //for each record
        ala[5].add(unwrap(rs.getString("COMPANY_NAME"))); //add that record's column entry in the appropriate arraylist
      }
    }
    // Column		Data
    //	0			Replacing Policy Key
    //	1			Replaced Policy Key
    //	2			Replacing Policy Number
    //	3			Replacing Company Name
    //	4			Replaced Policy Number
    //	5			Replaced Company Name
    //	6			Timestamp (sort order)
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return mslc;
  }
  /**
   * A special static fetch for population of the Company Search Results selection screen
   * Creation date: (9/22/2003 9:35:00 AM)
   * @return MatchedStringListCollection
   * @param searchField java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  MatchedStringListCollection selectCompanyCount(String searchField, Connection connection) throws SQLException {
    logger.debug("selectCompanyCount(String searchField, Connection connection)");
    MatchedStringListCollection mslc = new MatchedStringListCollection();
    Statement stmt = connection.createStatement();
    String stmtString = "";
    // create an ArrayList to hold the columns that will be returned from the query.
    ArrayList[] ala = new ArrayList[1];
    for (int i = 0; i < 1; i++) {
      ala[i] = new ArrayList();
    }
    // Construct SQL string
    stmtString =
      "SELECT COUNT(*) "
        + "FROM LIFEREPL.LR_INSURANCE_COMPANY "
        + "WHERE COMPANY_NAME LIKE '"
        + wrapLike(searchField)
        + "%' AND COMPANY_SHOW_IN_LIST = 'Y'";
    logger.debug(stmtString);
    ResultSet rs = stmt.executeQuery(stmtString);
    boolean someRecords = false;
    // start building the lists that will be added to the MSLC.
    // Column		Data
    //	0			the count
    while (rs.next()) { // for each record
      someRecords = true;
      ala[0].add(unwrap(rs.getString("COUNT(*)")));
    }
    if (!someRecords) {
      return null; // if nothing found, presentation wants null back...
    }
    for (int i = 0; i < 1; i++) { // for each column
      mslc.addList(ala[i]);
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return mslc;
  }
  /**
   * A special static fetch for population of the Company Search Results selection screen
   * Creation date: (9/15/2003 3:30:00 PM)
   * @return MatchedStringListCollection
   * @param searchField java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  MatchedStringListCollection selectCompanyList(String searchField, Connection connection) throws SQLException {
    logger.debug("selectCompanyList(String searchField, Connection connection)");
    MatchedStringListCollection mslc = new MatchedStringListCollection();
    Statement stmt = connection.createStatement();
    String stmtString = "";
    // create an ArrayList to hold the columns that will be returned from the query.
    ArrayList[] ala = new ArrayList[10];
    for (int i = 0; i < 10; i++) {
      ala[i] = new ArrayList();
    }
    // Construct SQL string
    stmtString =
      "SELECT INSURANCE_CO_KEY, COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, "
        + "COMPANY_STATE, COMPANY_ZIP, COMPANY_COUNTRY, CREATE_ROW_TS, UPDATE_ROW_TS "
        + "FROM LIFEREPL.LR_INSURANCE_COMPANY "
        + "WHERE COMPANY_NAME LIKE '"
        + wrapLike(searchField)
        + "%' AND COMPANY_SHOW_IN_LIST = 'Y'"
        + "ORDER BY COMPANY_NAME ASC";
    logger.debug(stmtString);
    ResultSet rs = stmt.executeQuery(stmtString);
    boolean someRecords = false;
    // start building the lists that will be added to the MSLC.
    // Column		Data
    //	0			INSURANCE_CO_KEY
    //	1			COMPANY_NAME
    //	2			COMPANY_ADDRESS_LINE_1
    //	3			COMPANY_ADDRESS_LINE_2
    //	4			COMPANY_CITY
    //	5			COMPANY_STATE
    //	6			COMPANY_ZIP
    //	7			COMPANY_COUNTRY
    //	8			CREATE_ROW_TS
    //	9			UPDATE_ROW_TS
    while (rs.next()) { // for each record
      someRecords = true;
      ala[0].add(unwrap(rs.getString("INSURANCE_CO_KEY")));
      ala[1].add(unwrap(rs.getString("COMPANY_NAME")));
      ala[2].add(unwrap(rs.getString("COMPANY_ADDRESS_LINE_1")));
      ala[3].add(unwrap(rs.getString("COMPANY_ADDRESS_LINE_2")));
      ala[4].add(unwrap(rs.getString("COMPANY_CITY")));
      ala[5].add(unwrap(rs.getString("COMPANY_STATE")));
      ala[6].add(unwrap(rs.getString("COMPANY_ZIP")));
      ala[7].add(unwrap(rs.getString("COMPANY_COUNTRY")));
      if (rs.getString("CREATE_ROW_TS") != null) {
        ala[8].add(unwrap(LrtsDateHandler.convertDateToDateTimeStampOutputString(rs.getTimestamp("CREATE_ROW_TS"))));
      } else {
        ala[8].add(unwrap(""));
      }
      if (rs.getString("UPDATE_ROW_TS") != null) {
        ala[9].add(unwrap(LrtsDateHandler.convertDateToDateTimeStampOutputString(rs.getTimestamp("UPDATE_ROW_TS"))));
      } else {
        ala[9].add(unwrap(""));
      }
    }
    if (!someRecords) {
      return null; // if nothing found, presentation wants null back...
    }
    for (int i = 0; i < 10; i++) { // for each column
      mslc.addList(ala[i]);
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return mslc;
  }


  //=========== start of PlanCode search====

/**
 * A special static fetch for population of the Plan Code Search Results selection screen
 * Creation date: (9/22/2003 9:35:00 AM)
 * @return MatchedStringListCollection
 * @param searchField java.lang.String
 * @param connection java.sql.Connection
 * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
 */
MatchedStringListCollection selectPlanCodeCount(String searchField, Connection connection)
	throws SQLException
{
	logger.debug("selectPlanCodeCount(String searchField, Connection connection)");

	MatchedStringListCollection mslc = new MatchedStringListCollection();

	Statement stmt = connection.createStatement();
	String stmtString = "";
	String stmtSearch= "";

    // create an ArrayList to hold the columns that will be returned from the query.
	ArrayList[] ala = new ArrayList[1];
	for (int i=0; i<1; i++)
	{
		ala[i] = new ArrayList();
	}


	// Construct SQL string

//	stmtString =
	stmtSearch=
	"SELECT COUNT(*) " +
	"FROM LIFEREPL.LR_PLAN_CD_GEN " ;

	if (searchField.length() >= 1) {
	stmtString= stmtSearch + "WHERE PLAN_CODE LIKE '" + wrapLike(searchField) + "%' ";
	}

	if (searchField.length() < 1){
	stmtString= stmtSearch + "WHERE PLAN_CODE LIKE '%' ";
	}

	logger.debug(stmtString);
	ResultSet rs = stmt.executeQuery(stmtString);

	boolean someRecords = false;

	// start building the lists that will be added to the MSLC.

	// Column		Data
	//	0			the count

	while (rs.next())
	{   // for each record
		someRecords = true;
		ala[0].add(unwrap(rs.getString("COUNT(*)")));
	}

	if (!someRecords)
	{
		return null;  // if nothing found, presentation wants null back...
	}


	for (int i=0; i<1; i++)
	{    // for each column
		mslc.addList(ala[i]);
	}


	try
	{
		rs.close();
	}
	catch (Exception ignore) {}

	try
	{
		stmt.close();
	}
	catch (Exception ignore) {}
	logger.debug("selectPlanCodeCount(Number of Plan Codes )" + mslc);
	return mslc;

}



/**
 * A special static fetch for population of the Plan Code Search Results selection screen
 * Creation date: (9/15/2003 3:30:00 PM)
 * @return MatchedStringListCollection
 * @param searchField java.lang.String
 * @param connection java.sql.Connection
 * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
 */
MatchedStringListCollection selectPlanCodeList(String searchField, Connection connection)
	throws SQLException
{
	logger.debug("selectPlanCodeList(String searchField, Connection connection)");

	MatchedStringListCollection mslc = new MatchedStringListCollection();

	Statement stmt = connection.createStatement();
	String stmtString = "";
	String stmtSearch= "";

    // create an ArrayList to hold the columns that will be returned from the query.
	ArrayList[] ala = new ArrayList[7];
	for (int i=0; i<7; i++)
	{
		ala[i] = new ArrayList();
	}


	// Construct SQL string
	//stmtString =
	stmtSearch =
	"SELECT PC.PLAN_CODE_KEY, PT.PRODUCT_TYPE_KEY, PC.PLAN_CODE,PT.PRODUCT_TYPE, PC.EFF_DATE, PC.END_EFF_DATE, PC.AFLIC_CURRENT_IND " +
	"FROM LIFEREPL.LR_PLAN_CD_GEN PC, LIFEREPL.LR_PRODUCT_TYPE PT ";

	if (searchField.length() >= 1) {
	stmtString= stmtSearch + "WHERE  PC.PLAN_CODE LIKE '" + wrapLike(searchField) + "%' AND PC.PRODUCT_TYPE_KEY = PT.PRODUCT_TYPE_KEY " +
	"ORDER BY PC.PLAN_CODE ASC";

	}

	if (searchField.length() < 1){
	stmtString= stmtSearch + "WHERE  PC.PLAN_CODE LIKE '%' AND PC.PRODUCT_TYPE_KEY = PT.PRODUCT_TYPE_KEY " +
	"ORDER BY PC.PLAN_CODE ASC";

	}


//	"SELECT INSURANCE_CO_KEY, COMPANY_NAME, COMPANY_ADDRESS_LINE_1, COMPANY_ADDRESS_LINE_2, COMPANY_CITY, COMPANY_STATE, COMPANY_ZIP, COMPANY_COUNTRY " +
//	"FROM LIFEREPL.LR_INSURANCE_COMPANY " +
//	"WHERE COMPANY_NAME LIKE '" + wrapLike(searchField) + "%' AND COMPANY_SHOW_IN_LIST = 'Y'" +
//	"ORDER BY COMPANY_NAME ASC";


	logger.debug(stmtString);
	ResultSet rs = stmt.executeQuery(stmtString);

	boolean someRecords = false;

	// start building the lists that will be added to the MSLC.

	// Column		Data
	//	0			PLAN_CODE_KEY
	//	1			PRODUCT_TYPE_KEY
	//	2			PLAN_CODE
	//	3			PRODUCT_TYPE
	//	4			EFF_DATE
	//	5			END_EFF_DATE
	//	6			AFLIC_CURRENT_IND

	while (rs.next())
	{   // for each record
		someRecords = true;
		ala[0].add(unwrap(rs.getString("PLAN_CODE_KEY")));
		ala[1].add(unwrap(rs.getString("PRODUCT_TYPE_KEY")));
		ala[2].add(unwrap(rs.getString("PLAN_CODE")));
		ala[3].add(unwrap(rs.getString("PRODUCT_TYPE")));


		if (rs.getString("EFF_DATE") != null)
		{
			ala[4].add(unwrap(LrtsDateHandler.convertDateToOutputString(rs.getTimestamp("EFF_DATE"))));

		}
		else
		{
			ala[4].add(unwrap(""));

		}

		if (rs.getString("END_EFF_DATE") != null)
		{
			ala[5].add(unwrap(LrtsDateHandler.convertDateToOutputString(rs.getTimestamp("END_EFF_DATE"))));

		}
		else
		{
			ala[5].add(unwrap(""));

		}

		ala[6].add(unwrap(rs.getString("AFLIC_CURRENT_IND")));
	}

	if (!someRecords)
	{
		return null;  // if nothing found, presentation wants null back...
	}


	for (int i=0; i<7; i++)
	{    // for each column
		mslc.addList(ala[i]);
	}


	try
	{
		rs.close();
	}
	catch (Exception ignore) {}

	try
	{
		stmt.close();
	}
	catch (Exception ignore) {}

	return mslc;
}


//=========== end for PlanCode search====



  /**
   * Unwrap takes values read from the database and
   * if null, formats as an empty string....
   * Creation date: (12/23/2002 2:56:09 PM)
   * @return java.lang.String
   * @param fieldToCheck java.lang.String
   */
  private String unwrap(String fieldToCheck) {
    logger.debug("unwrap(String fieldToCheck)");
    if ((fieldToCheck == null)) {
      return " ";
    } else {
      return fieldToCheck;
    }
  }
  /**
   * Check if the agent/district were ever valid through a table read.
   * Creation date: (12/12/2002 1:33:38 PM)
   * @return boolean
   * @param agent java.lang.String
   * @param district java.lang.String
   * @param connection java.sql.Connection
   * @exception java.sql.SQLException SQLExceptions are used to work errors up to the business layer.
   */
  boolean validateAgentDistrict(String agent, String district, Connection connection) throws SQLException {
	logger.debug("validateAgentDistrict(String agent, String district, Connection connection)");
    Statement stmt = connection.createStatement();
    String stmtString = "";
    //Construct SQL string
    stmtString = "SELECT * FROM AGENT WHERE AGT_CODE = '" + agent + "' AND DST_CODE = '" + district + "' ";
    logger.debug(stmtString);
    ResultSet rs = stmt.executeQuery(stmtString);
    boolean someRecords = false;
    if (rs.next()) {
      someRecords = true;
    }
    try {
      rs.close();
    } catch (Exception ignore) {}
    try {
      stmt.close();
    } catch (Exception ignore) {}
    return someRecords;
  }
  /**
   * Wrap handles incoming null fields and escaping any single quotes to make
   * sure they don't affect the SQL.  It also converts everything to uppercase for
   * the write.
   * Creation date: (12/23/2002 2:56:09 PM)
   * @return java.lang.String
   * @param fieldToCheck java.lang.String
   */
  private String wrap(String fieldToCheck) {
    logger.debug("wrap(String fieldToCheck)");
    if ((fieldToCheck == null) || (fieldToCheck.trim().equals(""))) {
      return "null";
    } else {
      String returnString = "'";
      StringTokenizer st = new StringTokenizer(fieldToCheck.toUpperCase().trim(), "'");
      while (st.hasMoreTokens()) {
        returnString += st.nextToken() + "''";
      }
      returnString = returnString.substring(0, returnString.length() - 2);
      return returnString + "'";
    }
  }
  /**
   * Wrap handles incoming null fields and escaping any single quotes to make
   * sure they don't affect the SQL.  It also converts everything to uppercase for
   * the write.
   * Creation date: (12/23/2002 2:56:09 PM)
   * @return java.lang.String
   * @param fieldToCheck java.lang.String
   */
  private String wrapLike(String fieldToCheck) {
    logger.debug("wrapLike(String fieldToCheck)");
    if ((fieldToCheck == null) || (fieldToCheck.trim().equals(""))) {
      return "null";
    } else {
      String returnString = "";
      StringTokenizer st = new StringTokenizer(fieldToCheck.toUpperCase().trim(), "'");
      while (st.hasMoreTokens()) {
        returnString += st.nextToken() + "''";
      }
      returnString = returnString.substring(0, returnString.length() - 2);
      return returnString + "";
    }
  }
  /**
   * WrapWithOperator calls wrap then adds
   * the equals operator as "=" or as "is" if a
   * null value is coming back.
   * Creation date: (12/23/2002 2:56:09 PM)
   * @return java.lang.String
   * @param fieldToCheck java.lang.String
   */
  private String wrapWithOperator(String fieldToCheck) {
    if (wrap(fieldToCheck).equals("null")) {
      return "is null";
    } else {
      return "= " + wrap(fieldToCheck);
    }
  }
}

/*
 * Group members: YOUR NAMES HERE
 * Instructions: For Project 2, implement all methods in this class, and test to confirm they behave as expected when the program is run.
 *Names: Meet Shah, Karan bhavsar
 */

package database;

import java.sql.*;
import java.util.*;
import dataClasses.*;
import dataClasses.Driver;
import menuOptions.CreateAccountMenuOption;

public class DatabaseMethods {
  private Connection conn;

  public DatabaseMethods(Connection conn) {
    this.conn = conn;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Retrieves information about all accounts
   * Returns: List of account objects
   */
  public ArrayList<Account> getAllAccounts() throws SQLException {
    ArrayList<Account> accounts = new ArrayList<Account>();

    try (Statement statement = conn.createStatement()) {
      String query = "SELECT ac.ID as Account_ID, ac.FIRST_NAME, ac.LAST_NAME, ac.PHONE_NUMBER, ac.EMAIL, ac.BIRTHDATE, "
          +
          "ad.ID as ADDRESS_ID, ad.STREET, ad.CITY, ad.PROVINCE, ad.POSTAL_CODE, " +
          "CASE WHEN pa.ID IS NULL THEN 0 ELSE 1 END AS PASSENGER_ID, " +
          "CASE WHEN dr.ID IS NULL THEN 0 ELSE 1 END AS DRIVER_ID " +
          "FROM accounts ac " +
          "INNER JOIN addresses ad ON ac.ADDRESS_ID = ad.Id " +
          "LEFT JOIN drivers dr ON dr.ID = ac.ID " +
          "LEFT JOIN passengers pa ON pa.ID = ac.ID";

      try (ResultSet resultSet = statement.executeQuery(query)) {
        while (resultSet.next()) {
          Account account = new Account(resultSet.getString("FIRST_NAME"), resultSet.getString("LAST_NAME"),
              resultSet.getString("STREET"), resultSet.getString("CITY"), resultSet.getString("PROVINCE"),
              resultSet.getString("POSTAL_CODE"), resultSet.getString("PHONE_NUMBER"), resultSet.getString("EMAIL"),
              resultSet.getString("BIRTHDATE"), resultSet.getBoolean("PASSENGER_ID"),
              resultSet.getBoolean("DRIVER_ID"));

          accounts.add(account);
        }
      }
      conn.close();
    }
    return accounts;
  }

  /*
   * Accepts: Email address of driver
   * Behaviour: Calculates the average rating over all rides performed by the
   * driver specified by the email address
   * Returns: The average rating value
   */
  public double getAverageRatingForDriver(String driverEmail) throws SQLException {
    double averageRating = 0.0;

    try {
      // SQL query with a parameter
      String sql = "SELECT AVG(RATING_FROM_DRIVER) " +
          "FROM accounts ac " +
          "INNER JOIN drivers d ON ac.ID = d.ID " +
          "INNER JOIN rides r ON d.ID = r.DRIVER_ID " +
          "WHERE ac.EMAIL = ?";

      PreparedStatement preparedStatement = conn.prepareStatement(sql);
      preparedStatement.setString(1, driverEmail);
      ResultSet resultSet = preparedStatement.executeQuery();

      if (resultSet.next()) {
        averageRating = resultSet.getDouble(1);
      }

      resultSet.close();
      preparedStatement.close();
      conn.close();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    // TODO: Implement

    return averageRating;
  }

  /*
   * Accepts: Account details, and passenger and driver specific details.
   * Passenger or driver details could be
   * null if account is only intended for one type of use.
   * Behaviour:
   * - Insert new account using information provided in Account object
   * - For non-null passenger/driver details, insert the associated data into the
   * relevant tables
   * Returns: Nothing
   */
  public void createAccount(Account account, Passenger passenger, Driver driver) throws SQLException {
    // TODO: Implement
    // Hint: Use the available insertAccount, insertPassenger, and insertDriver
    // methods
    int accountId = insertAccount(account);

    // If passenger is not null, insert passenger details
    if (passenger != null) {
      insertPassenger(passenger, accountId);
    }

    // If driver is not null, insert driver details
    if (driver != null) {
      insertDriver(driver, accountId);
    }
  }

  /*
   * Accepts: Account details (which includes address information)
   * Behaviour: Inserts the new account, as well as the account's address if it
   * doesn't already exist. The new/existing address should
   * be linked to the account
   * Returns: Id of the new account
   */
  public int insertAccount(Account account) throws SQLException {
    int accountId = -1;

    try {
      String insertAccountSQL = "INSERT INTO accounts (FIRST_NAME, LAST_NAME, BIRTHDATE, ADDRESS_ID, PHONE_NUMBER, EMAIL) VALUES (?, ?, ?, ?, ?, ?)";

      // Creating prepared statement
      PreparedStatement insertAccountStatement = conn.prepareStatement(insertAccountSQL,
          Statement.RETURN_GENERATED_KEYS);

      // Setting parameters for insertAccountStatement
      insertAccountStatement.setString(1, account.getFirstName());
      insertAccountStatement.setString(2, account.getLastName());
      insertAccountStatement.setString(3, account.getBirthdate());
      insertAccountStatement.setInt(4, insertAddressIfNotExists(account.getAddress()));
      insertAccountStatement.setString(5, account.getPhoneNumber());
      insertAccountStatement.setString(6, account.getEmail());

      // Executing insertAccountStatement
      int rowsAffected = insertAccountStatement.executeUpdate();
      if (rowsAffected > 0) {
        // Retrieve the generated ID
        ResultSet generatedKeys = insertAccountStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
          accountId = generatedKeys.getInt(1);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw e; // Rethrow the exception for handling in the calling method
    }
    return accountId;
  }

  /*
   * Accepts: Passenger details (should not be null), and account id for the
   * passenger
   * Behaviour: Inserts the new passenger record, correctly linked to the account
   * id
   * Returns: Id of the new passenger
   */
  public int insertPassenger(Passenger passenger, int accountId) throws SQLException {
    try {
      String insertPassengerSQL = "INSERT INTO passengers (ID, CREDIT_CARD_NUMBER) VALUES (?, ?)";

      // Creating prepared statement
      PreparedStatement insertPassengerStatement = conn.prepareStatement(insertPassengerSQL);

      // Setting parameters for insertPassengerStatement
      insertPassengerStatement.setInt(1, accountId);
      insertPassengerStatement.setString(2, passenger.getCreditCardNumber());

      // Executing insertPassengerStatement
      int rowsAffected = insertPassengerStatement.executeUpdate();
      if (rowsAffected > 0) {
        // Retrieve the generated ID
        ResultSet generatedKeys = insertPassengerStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
          accountId = generatedKeys.getInt(1);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw e; // Rethrow the exception for handling in the calling method
    }
    return accountId;
  }

  /*
   * Accepts: Driver details (should not be null), and account id for the driver
   * Behaviour: Inserts the new driver and driver's license record, correctly
   * linked to the account id
   * Returns: Id of the new driver
   */
  public int insertDriver(Driver driver, int accountId) throws SQLException {
    try {
      String insertDriverSQL = "INSERT INTO drivers (ID, LICENSE_ID) VALUES (?, ?)";
      // Creating prepared statement
      PreparedStatement insertDriverSQLStatement = conn.prepareStatement(insertDriverSQL);

      // Setting parameters for insertPassengerStatement
      insertDriverSQLStatement.setInt(1, accountId);
      insertDriverSQLStatement.setInt(2, insertLicense(driver.getLicenseNumber(), driver.getLicenseExpiryDate()));

      // Executing insertPassengerStatement
      int rowsAffected = insertDriverSQLStatement.executeUpdate();
      if (rowsAffected > 0) {
        // Retrieve the generated ID
        ResultSet generatedKeys = insertDriverSQLStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
          accountId = generatedKeys.getInt(1);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw e; // Rethrow the exception for handling in the calling method
    }
    return accountId;
  }

  /*
   * Accepts: Driver's license number and license expiry
   * Behaviour: Inserts the new driver's license record
   * Returns: Id of the new driver's license
   */
  public int insertLicense(String licenseNumber, String licenseExpiry) throws SQLException {
    int licenseId = -1;
    try {
      String insertAccountSQL = "insert into licenses (NUMBER, EXPIRY_DATE)"
          +
          "VALUES (?, ?)";

      // Creating prepared statement
      PreparedStatement insertAccountStatement = conn.prepareStatement(insertAccountSQL);

      // Setting parameters for insertAccountStatement
      insertAccountStatement.setString(1, licenseNumber);
      insertAccountStatement.setString(2, licenseExpiry);

      // Executing insertAccountStatement
      ResultSet resultSet = insertAccountStatement.executeQuery();
      if (resultSet.next()) {
        licenseId = resultSet.getInt("ID");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return licenseId;
  }

  /*
   * Accepts: Address details
   * Behaviour:
   * - Checks if an address with these properties already exists.
   * - If it does, gets the id of the existing address.
   * - If it does not exist, creates the address in the database, and gets the id
   * of the new address
   * Returns: Id of the address
   */
  public int insertAddressIfNotExists(Address address) throws SQLException {
    int addressId = -1;

    try {
      // Check if the address already exists
      String checkAddressSQL = "SELECT ID FROM addresses WHERE STREET = ? AND CITY = ? AND PROVINCE = ? AND POSTAL_CODE = ?";
      try (PreparedStatement checkAddressStatement = conn.prepareStatement(checkAddressSQL)) {
        checkAddressStatement.setString(1, address.getStreet());
        checkAddressStatement.setString(2, address.getCity());
        checkAddressStatement.setString(3, address.getProvince());
        checkAddressStatement.setString(4, address.getPostalCode());
        ResultSet resultSet = checkAddressStatement.executeQuery();

        if (resultSet.next()) {
          addressId = resultSet.getInt("ID");
        }
      }

      // If the address does not exist, insert it
      if (addressId == -1) {
        String insertAddressSQL = "INSERT INTO addresses (STREET, CITY, PROVINCE, POSTAL_CODE) VALUES (?, ?, ?, ?)";
        try (PreparedStatement insertAddressStatement = conn.prepareStatement(insertAddressSQL,
            Statement.RETURN_GENERATED_KEYS)) {
          insertAddressStatement.setString(1, address.getStreet());
          insertAddressStatement.setString(2, address.getCity());
          insertAddressStatement.setString(3, address.getProvince());
          insertAddressStatement.setString(4, address.getPostalCode());

          // Execute insert statement
          int rowsAffected = insertAddressStatement.executeUpdate();
          if (rowsAffected > 0) {
            // Retrieve the generated ID
            ResultSet generatedKeys = insertAddressStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
              addressId = generatedKeys.getInt(1);
            }
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return addressId;
  }

  /*
   * Accepts: Name of new favourite destination, email address of the passenger,
   * and the id of the address being favourited
   * Behaviour: Finds the id of the passenger with the email address, then inserts
   * the new favourite destination record
   * Returns: Nothing
   */

  public void insertFavouriteDestination(String favouriteName, String passengerEmail, int addressId)
      throws SQLException {

    String insertFavouriteDestinationQuery = "INSERT INTO favourite_locations (PASSENGER_ID, NAME, LOCATION_ID) VALUES (?, ?, ?)";
    PreparedStatement insertFavouriteDestinationStmt = conn.prepareStatement(insertFavouriteDestinationQuery);
    insertFavouriteDestinationStmt.setInt(1, getPassengerIdFromEmail(passengerEmail));
    insertFavouriteDestinationStmt.setString(2, favouriteName);
    insertFavouriteDestinationStmt.setInt(3, addressId);
    insertFavouriteDestinationStmt.executeUpdate();
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a driver exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkDriverExists(String email) throws SQLException {
    String query = "SELECT COUNT(*) FROM accounts A INNER JOIN drivers D ON A.ID=D.ID WHERE A.EMAIL=?";

    PreparedStatement pst = conn.prepareStatement(query);
    pst.setString(1, email);
    ResultSet rs = pst.executeQuery();
    if (rs.next()) {
      return rs.getInt(1) > 0;
    }
    return false;
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a passenger exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkPassengerExists(String email) throws SQLException {
    String query = "SELECT COUNT(*) FROM accounts A INNER JOIN passengers P ON A.ID=P.ID WHERE A.EMAIL=?";

    PreparedStatement pst = conn.prepareStatement(query);
    pst.setString(1, email);
    ResultSet rs = pst.executeQuery();
    if (rs.next()) {
      return rs.getInt(1) > 0;
    }
    return false;
  }

  /*
   * Accepts: Email address of passenger making request, id of dropoff address,
   * requested date/time of ride, and number of passengers
   * Behaviour: Inserts a new ride request, using the provided properties
   * Returns: Nothing
   */
  public void insertRideRequest(String passengerEmail, int dropoffLocationId, String date, String time,
      int numberOfPassengers) throws SQLException {
    int passengerId = this.getPassengerIdFromEmail(passengerEmail);
    int pickupAddressId = this.getAccountAddressIdFromEmail(passengerEmail);

    String query = "INSERT INTO ride_requests (PASSENGER_ID, PICKUP_LOCATION_ID, PICKUP_DATE, PICKUP_TIME, NUMBER_OF_RIDERS, DROPOFF_LOCATION_ID) VALUES (?, ?, ?, ?, ?, ?)";
    try (PreparedStatement pst = conn.prepareStatement(query)) {
      pst.setInt(1, passengerId);
      pst.setInt(2, pickupAddressId);
      pst.setString(3, date);
      pst.setString(4, time);
      pst.setInt(5, numberOfPassengers);
      pst.setInt(6, dropoffLocationId);
      pst.executeUpdate();
    }
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of passenger with specified email (assumes passenger
   * exists)
   * Returns: Id
   */
  public int getPassengerIdFromEmail(String passengerEmail) throws SQLException {
    int passengerId = -1;
    String sqlQuery = "SELECT P.ID " +
        "FROM accounts A " +
        "INNER JOIN passengers P ON A.ID = P.ID " +
        "WHERE A.EMAIL = ?";

    PreparedStatement stmt = conn.prepareStatement(sqlQuery);
    stmt.setString(1, passengerEmail);
    ResultSet rs = stmt.executeQuery();
    if (rs.next()) {
      passengerId = rs.getInt("ID");
    }
    return passengerId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of driver with specified email (assumes driver exists)
   * Returns: Id
   */
  public int getDriverIdFromEmail(String driverEmail) throws SQLException {
    int driverId = -1;
    String query = "SELECT d.ID FROM accounts A INNER JOIN drivers d ON A.ID=d.ID WHERE A.EMAIL = ?";

    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, driverEmail);
    ResultSet rs = stmt.executeQuery();

    if (rs.next()) {
      driverId = rs.getInt("ID");
    }

    return driverId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets the id of the address tied to the account with the provided
   * email address
   * Returns: Address id
   */
  public int getAccountAddressIdFromEmail(String email) throws SQLException {
    int addressId = -1;

    String query = "SELECT ADDRESS_ID FROM accounts A WHERE A.EMAIL = ?";
    PreparedStatement stmt = conn.prepareStatement(query);
    stmt.setString(1, email);
    ResultSet rs = stmt.executeQuery();

    if (rs.next()) {
      addressId = rs.getInt("ADDRESS_ID");
    }

    return addressId;
  }

  /*
   * Accepts: Email address of passenger
   * Behaviour: Gets a list of all the specified passenger's favourite
   * destinations
   * Returns: List of favourite destinations
   */
  public ArrayList<FavouriteDestination> getFavouriteDestinationsForPassenger(String passengerEmail)
      throws SQLException {
    ArrayList<FavouriteDestination> favouriteDestinations = new ArrayList<FavouriteDestination>();

    String sqlQuery = "SELECT fl.name, ac.* " +
        "FROM accounts A " +
        "INNER JOIN passengers p ON A.ID = P.ID " +
        "INNER JOIN addresses ac ON A.ADDRESS_ID = ac.ID " +
        "INNER JOIN favourite_locations fl ON p.ID = fl.PASSENGER_ID " +
        "WHERE A.EMAIL = ?";

    PreparedStatement selectPassengerIdStmt = conn.prepareStatement(sqlQuery);
    selectPassengerIdStmt.setString(1, passengerEmail);
    ResultSet resultSet = selectPassengerIdStmt.executeQuery();

    if (resultSet.next()) {
      FavouriteDestination fd = new FavouriteDestination(resultSet.getString("NAME"), resultSet.getInt("ID"),
          resultSet.getString("STREET"), resultSet.getString("CITY"), resultSet.getString("PROVINCE"),
          resultSet.getString("POSTAL_CODE"));

      favouriteDestinations.add(fd);
    } else {
      System.out.println("Passenger with email " + passengerEmail + " not found.");
    }

    return favouriteDestinations;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Gets a list of all uncompleted ride requests (i.e. requests
   * without an associated ride record)
   * Returns: List of all uncompleted rides
   */
  public ArrayList<RideRequest> getUncompletedRideRequests() throws SQLException {
    ArrayList<RideRequest> uncompletedRideRequests = new ArrayList<RideRequest>();

    String sqlQuery = "SELECT RR.ID, FIRST_NAME, LAST_NAME, ADPU.STREET AS PICKUP_STREET, " +
        "ADPU.CITY AS PICKUP_CITY, ADDO.STREET AS DROPOFF_STREET, " +
        "ADDO.CITY AS DROPOFF_CITY, PICKUP_DATE, PICKUP_TIME " +
        "FROM ride_requests RR " +
        "LEFT JOIN rides R ON R.REQUEST_ID = RR.ID " +
        "INNER JOIN passengers P ON RR.PASSENGER_ID = P.ID " +
        "INNER JOIN accounts AC ON P.ID = AC.ID " +
        "INNER JOIN addresses ADPU ON RR.PICKUP_LOCATION_ID = ADPU.ID " +
        "INNER JOIN addresses ADDO ON RR.DROPOFF_LOCATION_ID = ADDO.ID " +
        "WHERE R.ID IS NULL";

    try {
      PreparedStatement smt = conn.prepareStatement(sqlQuery);
      ResultSet resultSet = smt.executeQuery();
      while (resultSet.next()) {
        RideRequest rr = new RideRequest(resultSet.getInt("ID"), resultSet.getString("FIRST_NAME"),
            resultSet.getString("LAST_NAME"), resultSet.getString("PICKUP_STREET"), resultSet.getString("PICKUP_CITY"),
            resultSet.getString("DROPOFF_STREET"), resultSet.getString("DROPOFF_CITY"),
            resultSet.getString("PICKUP_DATE"), resultSet.getString("PICKUP_TIME"));
        uncompletedRideRequests.add(rr);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return uncompletedRideRequests;
  }

  /*
   * Accepts: Ride details
   * Behaviour: Inserts a new ride record
   * Returns: Nothing
   */
  public void insertRide(Ride ride) throws SQLException {
    // TODO: Implement
    // Hint: Use getDriverIdFromEmail

    String insertRowSQL = "INSERT INTO rides (DRIVER_ID, REQUEST_ID, ACTUAL_START_DATE, ACTUAL_START_TIME, "
        + "ACTUAL_END_DATE, ACTUAL_END_TIME, RATING_FROM_DRIVER, RATING_FROM_PASSENGER, DISTANCE, CHARGE) "
        + "VALUES (?,?,?,?,?,?,?,?,?,?);";

    try {
      PreparedStatement insertStatement = conn.prepareStatement(insertRowSQL);
      insertStatement.setInt(1, getDriverIdFromEmail(ride.getDriverEmail()));
      insertStatement.setInt(2, ride.getRideRequestId());
      insertStatement.setString(3, ride.getStartDate());
      insertStatement.setString(4, ride.getStartTime());
      insertStatement.setString(5, ride.getEndDate());
      insertStatement.setString(6, ride.getEndTime());
      insertStatement.setInt(7, ride.getRatingFromDriver());
      insertStatement.setInt(8, ride.getRatingFromPassenger());
      insertStatement.setDouble(9, ride.getDistance());
      insertStatement.setDouble(10, ride.getCharge());
      insertStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}

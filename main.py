import random
from faker import Faker
import mysql.connector
from mysql.connector import Error

# Initialize Faker
fake = Faker()

# Connection details
host = '192.168.1.5'
port = 3306
user = 'elene'
password = 'some_pass'
database = 'dwh'  # Change this to your database name

# Function to create a MySQL connection
def create_connection():
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

# Function to execute a single SQL query
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"Error: '{e}'")
        connection.rollback()

# Generate Customers data
customers = []
for _ in range(100):
    customers.append((
        fake.first_name(),
        fake.last_name(),
        fake.date_of_birth(minimum_age=18, maximum_age=90),
        random.choice(['Male', 'Female']),
        fake.email(),
        fake.phone_number(),
        fake.address(),
        fake.city(),
        fake.state(),
        fake.zipcode()
    ))

# Insert Customers data
connection = create_connection()
customers_query = """
INSERT INTO BDB_Customers ( FirstName, LastName, DateOfBirth, Gender, Email, PhoneNumber, Address, City, State, ZipCode)
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for customer in customers:
    execute_query(connection, customers_query, customer)

# Generate Accounts data
accounts = []
for account_id in range(1, 101):
    accounts.append((
        random.randint(1, 100),
        random.choice(['Savings', 'Checking']),
        round(random.uniform(1000, 100000), 2),
        fake.date_between(start_date='-5y', end_date='today')
    ))

# Insert Accounts data
accounts_query = """
INSERT INTO BDB_Accounts (CustomerID, AccountType, Balance, DateOpened)
VALUES (%s, %s, %s, %s)
"""
for account in accounts:
    execute_query(connection, accounts_query, account)

# Generate Loans data
loans = []
for loan_id in range(1, 201):
    loans.append((
        random.randint(1, 100),
        random.choice(['Personal', 'Mortgage', 'Auto']),
        round(random.uniform(5000, 50000), 2),
        round(random.uniform(3, 15), 2),
        random.randint(12, 360),
        fake.date_between(start_date='-5y', end_date='today')
    ))

# Insert Loans data
loans_query = """
INSERT INTO BDB_Loans (CustomerID, LoanType, LoanAmount, InterestRate, LoanTerm, StartDate)
VALUES (%s, %s, %s, %s, %s, %s)
"""
for loan in loans:
    execute_query(connection, loans_query, loan)

# Generate Transactions data
transactions = []
for transaction_id in range(1, 1001):
    transactions.append((
        random.randint(1, 100),
        fake.date_between(start_date='-5y', end_date='today'),
        random.choice(['Deposit', 'Withdrawal', 'Payment']),
        round(random.uniform(50, 5000), 2)
    ))

# Insert Transactions data
transactions_query = """
INSERT INTO BDB_Transactions (AccountID, TransactionDate, TransactionType, Amount)
VALUES (%s, %s, %s, %s)
"""
for transaction in transactions:
    execute_query(connection, transactions_query, transaction)

# Generate Employees data
employees = []
for _ in range(50):
    employees.append((
        fake.first_name(),
        fake.last_name(),
        fake.job(),
        fake.email(),
        fake.phone_number(),
        fake.date_between(start_date='-10y', end_date='today')
    ))

# Insert Employees data
employees_query = """
INSERT INTO BDB_Employees (FirstName, LastName, Position, Email, PhoneNumber, HireDate)
VALUES (%s, %s, %s, %s, %s, %s)
"""
for employee in employees:
    execute_query(connection, employees_query, employee)

# Generate LoanEmployees data
loan_employees = []
for loan_employee_id in range(1, 201):
    loan_employees.append((
        random.randint(1, 200),
        random.randint(1, 50),
        random.choice(['Originator', 'Approver'])
    ))

# Insert LoanEmployees data
loan_employees_query = """
INSERT INTO BDB_LoanEmployees (LoanID, EmployeeID, Role)
VALUES (%s, %s, %s)
"""
for loan_employee in loan_employees:
    execute_query(connection, loan_employees_query, loan_employee)

# Generate Branches data
branches = []
for branch_id in range(1, 11):
    branches.append((
        fake.company(),
        fake.address(),
        fake.city(),
        fake.state(),
        fake.zipcode()
    ))

# Insert Branches data
branches_query = """
INSERT INTO BDB_Branches (BranchName, BranchAddress, BranchCity, BranchState, BranchZipCode)
VALUES (%s, %s, %s, %s, %s)
"""
for branch in branches:
    execute_query(connection, branches_query, branch)

# Generate Payments data
payments = []
for payment_id in range(1, 501):
    payments.append((
        random.randint(1, 200),
        fake.date_between(start_date='-5y', end_date='today'),
        round(random.uniform(100, 5000), 2),
        random.choice(['Cash', 'Check', 'Electronic'])
    ))

# Insert Payments data
payments_query = """
INSERT INTO BDB_Payments (LoanID, PaymentDate, PaymentAmount, PaymentMethod)
VALUES (%s, %s, %s, %s)
"""
for payment in payments:
    execute_query(connection, payments_query, payment)

# Generate CustomerInteractions data
interactions = []
for interaction_id in range(1, 501):
    interactions.append((
        random.randint(1, 100),
        random.randint(1, 50),
        random.choice(['Email', 'Phone Call', 'In-Person', 'Chat']),
        fake.date_between(start_date='-5y', end_date='today'),
        fake.paragraph(nb_sentences=3)
    ))

# Insert CustomerInteractions data
interactions_query = """
INSERT INTO BDB_CustomerInteractions (CustomerID, EmployeeID, InteractionType, InteractionDate, Notes)
VALUES (%s, %s, %s, %s, %s)
"""
for interaction in interactions:
    execute_query(connection, interactions_query, interaction)

# Generate ProductUsage data
usages = []
for usage_id in range(1, 501):
    usages.append((
        random.randint(1, 100),
        random.choice(['Credit Card', 'Loan', 'Savings Account']),
        round(random.uniform(100, 10000), 2),
        fake.date_between(start_date='-5y', end_date='today')
    ))

# Insert ProductUsage data
usages_query = """
INSERT INTO BDB_ProductUsage (CustomerID, ProductType, UsageAmount, UsageDate)
VALUES (%s, %s, %s, %s)
"""
for usage in usages:
    execute_query(connection, usages_query, usage)

# Close the connection
connection.close()

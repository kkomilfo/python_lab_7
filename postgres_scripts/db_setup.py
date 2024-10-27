import logging
from datetime import date

from prettytable import PrettyTable
from sqlalchemy import create_engine, text, Column, String, Integer, CheckConstraint, ForeignKey, Date, Float, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)

LOGGER.info("Start operations")

# Define the database engine using pg8000 as the adapter
password = "QazWsx%40Edc1234"

engine = create_engine(f'postgresql+pg8000://postgres:{password}@postgres:5432/clinic_db')

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# # Define models (tables)
class Patient(Base):
    __tablename__ = 'patients'
    card_number = Column(Integer, primary_key=True)
    last_name = Column(String(50))
    first_name = Column(String(50))
    middle_name = Column(String(50))
    address = Column(String)
    phone = Column(String(10))
    birth_year = Column(Integer)
    category = Column(String(10), CheckConstraint("category IN ('child', 'adult')"))
#
class HospitalStay(Base):
    __tablename__ = 'hospital_stays'
    stay_code = Column(Integer, primary_key=True)
    patient_card_number = Column(Integer, ForeignKey('patients.card_number'))
    admission_date = Column(Date)
    days_in_hospital = Column(Integer)
    daily_cost = Column(Float)
    discount_percent = Column(Float)
    doctor_code = Column(Integer, ForeignKey('doctors.doctor_code'))
#
class Doctor(Base):
    __tablename__ = 'doctors'
    doctor_code = Column(Integer, primary_key=True)
    last_name = Column(String(50))
    first_name = Column(String(50))
    middle_name = Column(String(50))
    specialization = Column(String(50))
    experience_years = Column(Integer)

inspector = inspect(engine)
if "patients" and "hospital_stays" and "doctors" in inspector.get_table_names():
    LOGGER.info("Exists")
else:
    LOGGER.info("Create all tables operations")
    Base.metadata.create_all(engine)
    doctors_data = [
        Doctor(
            doctor_code=1,
            last_name="Ivanov",
            first_name="Ivan",
            middle_name="Ivanovich",
            specialization="therapist",
            experience_years=10
        ),
        Doctor(
            doctor_code=2,
            last_name="Petrov",
            first_name="Petr",
            middle_name="Petrovich",
            specialization="surgeon",
            experience_years=15
        ),
        Doctor(
            doctor_code=3,
            last_name="Sidorov",
            first_name="Sidr",
            middle_name="Sidorovich",
            specialization="lor",
            experience_years=8
        ),
        Doctor(
            doctor_code=4,
            last_name="Nikolaev",
            first_name="Nikolai",
            middle_name="Nikolayevich",
            specialization="therapist",
            experience_years=12
        )
    ]

    # Sample Patients Data
    patients_data = [
        Patient(card_number=1, last_name="Doe", first_name="John", middle_name="A", address="123 Main St",
                phone="1234567890", birth_year=1990, category="adult"),
        Patient(card_number=2, last_name="Smith", first_name="Anna", middle_name="B", address="456 Elm St",
                phone="0987654321", birth_year=1985, category="adult"),
        Patient(card_number=3, last_name="Johnson", first_name="Emily", middle_name="C", address="789 Maple St",
                phone="5432167890", birth_year=2005, category="child"),
        Patient(card_number=4, last_name="Brown", first_name="Michael", middle_name="D", address="321 Oak St",
                phone="1029384756", birth_year=1978, category="adult"),
        Patient(card_number=5, last_name="Wilson", first_name="James", middle_name="E", address="159 Pine St",
                phone="5647382910", birth_year=1992, category="adult"),
        Patient(card_number=6, last_name="Taylor", first_name="Linda", middle_name="F", address="753 Cedar St",
                phone="9182736450", birth_year=2000, category="child"),
        Patient(card_number=7, last_name="Anderson", first_name="Robert", middle_name="G", address="258 Birch St",
                phone="1357924680", birth_year=1995, category="adult"),
        Patient(card_number=8, last_name="Thomas", first_name="Barbara", middle_name="H", address="147 Spruce St",
                phone="2468135790", birth_year=1980, category="adult"),
        Patient(card_number=9, last_name="Jackson", first_name="Chris", middle_name="I", address="369 Fir St",
                phone="8642097531", birth_year=2010, category="child")
    ]

    # Sample Hospital Stays Data
    hospital_stays_data = [
        HospitalStay(stay_code=1, patient_card_number=1, admission_date=date(2023, 1, 10), days_in_hospital=3,
                     daily_cost=100.0, discount_percent=10.0, doctor_code=1),
        HospitalStay(stay_code=2, patient_card_number=2, admission_date=date(2023, 2, 15), days_in_hospital=5,
                     daily_cost=200.0, discount_percent=5.0, doctor_code=2),
        HospitalStay(stay_code=3, patient_card_number=3, admission_date=date(2023, 3, 20), days_in_hospital=1,
                     daily_cost=150.0, discount_percent=0.0, doctor_code=3),
        HospitalStay(stay_code=4, patient_card_number=4, admission_date=date(2023, 4, 25), days_in_hospital=7,
                     daily_cost=120.0, discount_percent=15.0, doctor_code=1),
        HospitalStay(stay_code=5, patient_card_number=5, admission_date=date(2023, 5, 30), days_in_hospital=2,
                     daily_cost=180.0, discount_percent=10.0, doctor_code=2),
        HospitalStay(stay_code=6, patient_card_number=6, admission_date=date(2023, 6, 5), days_in_hospital=4,
                     daily_cost=130.0, discount_percent=5.0, doctor_code=3),
        HospitalStay(stay_code=7, patient_card_number=7, admission_date=date(2023, 7, 10), days_in_hospital=6,
                     daily_cost=200.0, discount_percent=0.0, doctor_code=4),
        HospitalStay(stay_code=8, patient_card_number=8, admission_date=date(2023, 8, 15), days_in_hospital=2,
                     daily_cost=110.0, discount_percent=10.0, doctor_code=1),
        HospitalStay(stay_code=9, patient_card_number=1, admission_date=date(2023, 9, 20), days_in_hospital=5,
                     daily_cost=100.0, discount_percent=15.0, doctor_code=2),
        HospitalStay(stay_code=10, patient_card_number=5, admission_date=date(2023, 10, 1), days_in_hospital=3,
                     daily_cost=150.0, discount_percent=0.0, doctor_code=3),
        HospitalStay(stay_code=11, patient_card_number=4, admission_date=date(2023, 10, 15), days_in_hospital=4,
                     daily_cost=120.0, discount_percent=5.0, doctor_code=4),
        HospitalStay(stay_code=12, patient_card_number=2, admission_date=date(2023, 11, 5), days_in_hospital=2,
                     daily_cost=200.0, discount_percent=10.0, doctor_code=1),
        HospitalStay(stay_code=13, patient_card_number=3, admission_date=date(2023, 11, 10), days_in_hospital=1,
                     daily_cost=150.0, discount_percent=0.0, doctor_code=2),
        HospitalStay(stay_code=14, patient_card_number=6, admission_date=date(2023, 12, 1), days_in_hospital=3,
                     daily_cost=130.0, discount_percent=10.0, doctor_code=3),
        HospitalStay(stay_code=15, patient_card_number=7, admission_date=date(2023, 12, 10), days_in_hospital=4,
                     daily_cost=200.0, discount_percent=0.0, doctor_code=4),
        HospitalStay(stay_code=16, patient_card_number=8, admission_date=date(2024, 1, 5), days_in_hospital=2,
                     daily_cost=110.0, discount_percent=10.0, doctor_code=1),
        HospitalStay(stay_code=17, patient_card_number=9, admission_date=date(2024, 1, 20), days_in_hospital=3,
                     daily_cost=100.0, discount_percent=5.0, doctor_code=2)
    ]

    # Insert Doctors
    if "doctors" in inspector.get_table_names():
        for doctor in doctors_data:
            session.add(doctor)

    # Insert Patients
    if "patients" in inspector.get_table_names():
        for patient in patients_data:
            session.add(patient)

    session.commit()

    # Insert Hospital Stays
    if "hospital_stays" in inspector.get_table_names():
        for stay in hospital_stays_data:
            session.add(stay)

    # Commit the transaction
    session.commit()



LOGGER.info("Complete all operations again")

def execute_query(query, params=None):
    """Виконує SQL запит і повертає результати."""
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        return result.fetchall(), result.keys()

def print_results(title, results):
    """Виводить заголовок та результати запиту у форматованому вигляді."""
    rows, headers = results
    table = PrettyTable(headers)
    table.align = "l"  # Вирівнювання всіх стовпців по лівому краю
    for row in rows:
        table.add_row(row)
    LOGGER.info(f"\n{title}:\n{table}")

# 1. Відобразити всіх пацієнтів, які народилися після 1998 року.
query1 = """
SELECT * 
FROM patients 
WHERE birth_year > 1998 
ORDER BY last_name;
"""
print_results("Пацієнти, які народилися після 1998 року", execute_query(query1))

# 2. Порахувати кількість пацієнтів дитячої та дорослої категорії.
query2 = """
SELECT 
    category, 
    COUNT(*) AS patient_count 
FROM patients 
GROUP BY category;
"""
print_results("Кількість пацієнтів кожної категорії", execute_query(query2))

# 3. Порахувати суму лікування та суму лікування з урахуванням пільги для кожного пацієнта.
query3 = """
SELECT 
    h.patient_card_number, 
    SUM(h.days_in_hospital * h.daily_cost) AS total_treatment_cost, 
    SUM(h.days_in_hospital * h.daily_cost * (1 - h.discount_percent / 100)) AS total_discounted_cost 
FROM hospital_stays h 
GROUP BY h.patient_card_number;
"""
print_results("Сума лікування та лікування з урахуванням пільги", execute_query(query3))

# 4. Відобразити всі звернення до лікаря заданої спеціалізації (запит з параметром).
specialization = 'therapist'  # Задайте спеціалізацію
query4 = """
SELECT 
    hs.stay_code,
    hs.patient_card_number,
    hs.admission_date,
    hs.days_in_hospital,
    hs.daily_cost,
    hs.discount_percent,
    d.doctor_code AS doctor_id,
    d.last_name AS doctor_last_name,
    d.first_name AS doctor_first_name,
    d.middle_name AS doctor_middle_name,
    d.specialization,
    d.experience_years
FROM 
    hospital_stays hs
JOIN 
    doctors d ON hs.doctor_code = d.doctor_code
WHERE 
    d.specialization = :specialization
"""
print_results(f"Звернення до лікаря спеціалізації: {specialization}", execute_query(query4, {'specialization': specialization}))

# 5. Порахувати кількість звернень пацієнтів до кожного лікаря.
query5 = """
SELECT 
    d.doctor_code AS doctor_code, 
    d.last_name AS last_name, 
    d.first_name AS first_name, 
    COUNT(h.stay_code) AS visit_count 
FROM doctors d 
LEFT JOIN hospital_stays h ON d.doctor_code = h.doctor_code 
GROUP BY d.doctor_code, d.last_name, d.first_name
ORDER BY visit_count DESC;  -- Optional: Order by visit count in descending order
"""
print_results("Кількість звернень пацієнтів до лікарів", execute_query(query5))

# 6. Порахувати кількість пацієнтів кожної категорії, які лікувалися у лора, терапевта, хірурга.
query6 = """
SELECT 
    p.category AS patient_category, 
    d.specialization AS doctor_specialization, 
    COUNT(p.card_number) AS patient_count 
FROM patients p 
JOIN hospital_stays h ON p.card_number = h.patient_card_number 
JOIN doctors d ON h.doctor_code = d.doctor_code 
WHERE d.specialization IN ('lor', 'therapist', 'surgeon') 
GROUP BY p.category, d.specialization
ORDER BY p.category, d.specialization;  -- Optional: Order by patient category and specialization
"""
print_results("Кількість пацієнтів кожної категорії, які лікувалися у лікарів", execute_query(query6))

query7 = """
SELECT * FROM patients;
"""
print_results("Pacient", execute_query(query7))

query8 = """
SELECT * FROM hospital_stays;
"""
print_results("hospital_stays", execute_query(query8))

query9 = """
SELECT * FROM doctors;
"""
print_results("Doctors", execute_query(query9))


session.close()
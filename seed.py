from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from faker import Faker
from conf.models import Student, Group, Subject, Teacher, Grade
import random

# Підключення до бази даних
engine = create_engine('sqlite:///students.db')
Session = sessionmaker(bind=engine)
session = Session()

# Ініціалізація Faker
fake = Faker()

# Генерування даних
groups = []
subjects = []
teachers = []
students = []
grades = []

# Створення груп
for i in range(3):
    group = Group(name=f'Група {i+1}')
    groups.append(group)
    session.add(group)

# Створення предметів
for i in range(5, 9):
    subject = Subject(name=f'Предмет {i}')
    subjects.append(subject)
    session.add(subject)

# Створення викладачів
for i in range(3, 6):
    teacher = Teacher(name=f'Викладач {i}')
    teachers.append(teacher)
    session.add(teacher)

# Створення студентів та їх оцінок
for _ in range(30, 51):
    student = Student(name=fake.name(), group=random.choice(groups))
    students.append(student)
    session.add(student)
    for subject in subjects:
        for _ in range(20):
            grade = Grade(value=random.randint(60, 100), student=student, subject=subject, teacher=random.choice(teachers))
            session.add(grade)

# Збереження даних у базі даних
session.commit()

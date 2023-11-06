from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02(subject_id: int):
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == subject_id).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result




def select_03(subject_id: int):
    """"
    SELECT g.name AS group_name, s.name AS subject_name, AVG(grade) AS average_score
    FROM grades gr
    JOIN students st ON gr.student_id = st.id
    JOIN groups g ON st.group_id = g.id
    JOIN subjects s ON gr.subject_id = s.id
    WHERE s.id = 1
    GROUP BY g.name, s.name;
    """
    result = session.query(Group.name, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Group).join(Subject).filter(Subject.id == subject_id).group_by(Group.name, Subject.name).all()
    return result


def select_04():
    """"
    SELECT ROUND(AVG(grade),2) as average_grade
    FROM grades
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).all()
    return result


def select_05(teacher_id):
    """"
    SELECT s.name AS subject_name
    FROM subjects s
    WHERE teacher_id = 1;
    """
    result = session.query(Subject.name) \
        .join(Teacher, Teacher.id == Subject.teacher_id) \
        .filter(Teacher.id == teacher_id).all()
    return result


def select_06(group_id):
    """"
    SELECT fullname AS student_name
    FROM students
    WHERE group_id = 1;
    """
    result = session.query(Student.fullname) \
        .join(Group, Group.id == Student.group_id) \
        .filter(Group.id == group_id).all()
    return result


def select_07(group_id, subject_id):
    """"
    SELECT students.fullname AS student_name, grades.grade
    FROM students
    JOIN groups ON students.group_id = groups.id
    JOIN grades ON students.id = grades.student_id
    JOIN subjects ON grades.subject_id = subjects.id
    WHERE groups.id = 1 AND subjects.id = 1;
    """

    result = session.query(Student.fullname, Grade.grade) \
        .join(Group).join(Grade).join(Subject) \
        .filter(and_(Group.id == group_id, Subject.id == subject_id)) \
        .group_by(Student.fullname, Grade.grade).all()
    return result


def select_08(teacher_id: int):
    result = session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
                    .select_from(Teacher).join(Subject).join(Grade)\
                    .filter(Teacher.id == teacher_id)\
                    .group_by(Teacher.fullname, Subject.name).all()
    return result


def select_09(student_id):
    """
    SELECT s.name AS subject_name
    FROM subjects s
    JOIN grades g ON s.id = g.subject_id
    JOIN students st ON g.student_id = st.id
    WHERE st.id = 1
    GROUP BY subject_name;
    :return:
    """
    result = session.query(Subject.name) \
        .select_from(Subject).join(Grade).join(Student).filter(Student.id == student_id).group_by(Subject.name).all()
    return result


def select_10(group_id, teacher_id):
    """"
    SELECT s.name AS subject_name
    FROM students st
    JOIN grades g ON st.id = g.student_id
    JOIN subjects s ON g.subject_id = s.id
    JOIN teachers t ON s.teacher_id = t.id
    WHERE g.student_id = 1 AND t.id = 1
    GROUP BY subject_name;
    """
    result = session.query(Subject.name) \
        .select_from(Student).join(Grade).join(Subject).join(Teacher) \
        .filter(and_(Student.group_id == group_id, Teacher.id == teacher_id)).group_by(Subject.name).all()
    return result


def select_11(student_id, teacher_id):
    """"
    SELECT ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students st ON g.student_id = st.id
    JOIN subjects s ON g.subject_id = s.id
    JOIN teachers t ON s.teacher_id = t.id
    WHERE s.id = 1 AND t.id = 1
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Subject).join(Teacher) \
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)).all()
    return result

def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return result


def select_12(grade_subjects_id, student_group_id):
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subjects_id == grade_subjects_id, Student.group_id == student_group_id
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subjects_id == grade_subjects_id, Student.group_id == student_group_id, Grade.grade_date == subquery)).all()

    return result


if __name__ == '__main__':
    print(f'1.Знайти 5 студентів із найбільшим середнім балом з усіх предметів:\n{select_01()}\n')
    print(f'2.Знайти студента із найвищим середнім балом з певного предмета:\n{select_02(1)}\n')
    print(f'3.Знайти середній бал у групах з певного предмета:\n{select_03(1)}\n')
    print(f'4.Знайти середній бал на потоці (по всій таблиці оцінок):\n{select_04()}\n')
    print(f'5.Знайти які курси читає певний виклада:\n{select_05(1)}\n')
    print(f'6.Знайти список студентів у певній групі:\n{select_06(1)}\n')
    print(f'7.Знайти оцінки студентів у окремій групі з певного предмета:\n{select_07(1, 1)}\n')
    print(f'8.Знайти середній бал, який ставить певний викладач зі своїх предметів:\n{select_08(1)}\n')
    print(f'9.Знайти список курсів, які відвідує певний студент:\n{select_09(2)}\n')
    print(f'10.Список курсів, які певному студенту читає певний викладач:\n{select_10(1, 2)}')
    print(f'11.Середній бал, який певний викладач ставить певному студентові:\n{select_11(1, 2)}')
    print(f'12.Оцінки студентів у певній групі з певного предмета на останньому занятті:\n{select_12(2, 3)}')

package reflect;

public class Man {
    private Student student;

    public Man() {
    }

    public Man(Student student) {
        this.student = student;
    }

    public Student getStudent() {
        return student;
    }

    public void setStudent(Student student) {
        this.student = student;
    }
}

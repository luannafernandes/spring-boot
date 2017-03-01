package feature.salary;

//import com.hascode.tutorial.cucumber.salary.Employee;
//import com.hascode.tutorial.cucumber.salary.SalaryManager;

import com.my.test.go.MvcRestClient;
import com.my.test.go.User;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.Then;
import cucumber.api.java.en.When;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class UserSteps {

    MvcRestClient client;
    Optional<User> user;

    @Given("^there is a user with Id (\\d+)")
    public void addNewBook(final Integer userId) {
        client = new MvcRestClient();
    }

    @When("^get by Id = (\\d+)$")
    public void setSearchParameters(final Integer userId) {
        user = client.getUserById(userId);
    }

    @Then("^User should have the name '(.+)'$")
    public void verifyName(final String name) {
        assertThat(user.get().getName(), equalTo(name));
    }

    @Then("User should have the name (\\d+)$")
    public void verifyId(Integer userId) {
        assertThat(user.get().getUserId(), equalTo(userId));
    }
}

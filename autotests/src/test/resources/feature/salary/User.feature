Feature: getting user
	
  Scenario: get user by Id 12
    Given there is a user with Id 12
    When get by Id = 12
    Then User should have the name 'name'
    Then there is a user with Id 12

  Scenario: get user by Id 13
    Given there is a user with Id 13
    When get by Id = 13
    Then User should have the name 'name13'
    Then there is a user with Id 13
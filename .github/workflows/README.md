## PR TESTS

PR tests are tests created for two main reasons: 
1. to make sure we keep the integrity of the packages -> integration testing
2. to be able to check if the code in development does what it is designed to do -> unit testing

### Integration tests:
All integration test related code is under the folder integration_tests within pr_tests. They are designed to be run for each PR.

### Unit tests:
Unit test related code is under the folder unit_tests within pr_tests/models. They are designed to be run only when a particular feature is developed and optionally also when the PR is opened against main. They are referenced by the macro to be tested and may contain multiple tests. Testing is usually achieved by comparing the expected_<macro_name>_macro sql against test_<macro_name>_macro sql outputs. Both models are executed and compared using dbt_utils.equality check. 

To schedule the tests when ready you can add them in the corresponding script under .scripts folder then reference it in the pr_tests.yml as a github workflow.





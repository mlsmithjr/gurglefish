/**
 * Created by mark on 6/10/15.
 */

var gurglefishApp = angular.module('gurglefish', []);

gurglefishApp.controller('CreateDBController', ['$scope','$http', function($scope,$http) {
    $scope.env = { 'name' : 'undefined', 'logincustom': false };


    $scope.testEnv = function() {
        $('#btn-testenv').prop('disabled', true);
        $('#btn-testenv').button('loading');
        $http.post('/services/testEnv', $scope.env).success(function(data) {
            $('#btn-testenv').prop('disabled', false);
            $('#btn-testenv').button('reset');
            console.log(data);
            if (!data.success) {
                bootbox.dialog({
                  message: data.message,
                  title: "Salesforce Error",
                  buttons: {
                    success: {
                      label: "Ok",
                      className: "btn-success",
                      callback: function() {
                      }
                    }
                  }
                });
            }
            else {
                bootbox.alert('Test Successful', function() {
                });
            }
        });
    };


    $scope.saveDB = function() {
        $http.post('/services/saveEnv', $scope.env).success(function(data) {
            console.log(data);
            if (!data.success) {
                bootbox.dialog({
                  message: data.message,
                  title: "Unexpected Error",
                  buttons: {
                    success: {
                      label: "Ok",
                      className: "btn-success",
                      callback: function() {
                      }
                    }
                  }
                });
            }
            else {
                bootbox.alert('Saved', function() {
                });
                $scope.env['envid'] = data.envid;
            }
        });
    };


}]);

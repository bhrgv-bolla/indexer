package org.bbolla.db.maintenance;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.bbolla.db.indexer.impl.Server;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * @author bbolla on 12/26/18
 */
@RestController
@RequestMapping("/maintenance/cluster")
@Slf4j
public class MaintenanceRestController {

    @Autowired
    private Server server;

    private static final String PASSWORD_HASHED = "33F6ECB99522960D01FD1CC09DE62B49"; //TODO different per deployment


    @PutMapping("/activate")
    public ResponseEntity<Object> activateCluster(@RequestHeader("password") String pass) {
        String hashed = digestPass(pass);
        if (PASSWORD_HASHED.equals(hashed)) {
            boolean result = server.activate();
            if (result) {
                return ResponseEntity.ok(server.nodes());
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong");
            }
        } else {
            return ResponseEntity.badRequest().body("Secret Key Missing/ Wrong");
        }
    }


    @GetMapping("/status")
    public Boolean status() {
        return server.isActive();
    }

    @PutMapping("/deactivate")
    public ResponseEntity<Object> deactivateCluster(@RequestHeader("password") String pass) {
        String hashed = digestPass(pass);
        if (PASSWORD_HASHED.equals(hashed)) {
            boolean result = server.deactivate();
            if (result) {
                return ResponseEntity.ok("deactivated");
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong");
            }
        } else {
            return ResponseEntity.badRequest().body("Secret Key Missing/ Wrong");
        }
    }

    @PostMapping("/node/add/{id}")
    public ResponseEntity<Object> addNodeToCluster(@PathVariable("id") String nodeId,
                                                   @RequestHeader("password") String pass) {
        verify(pass);
        return ResponseEntity.ok(server.addNode(nodeId));
    }

    @PostMapping("/node/remove/{id}")
    public ResponseEntity<Object> removeNodeFromCluster(@PathVariable("id") String nodeId,
                                                        @RequestHeader("password") String pass) {
        verify(pass);
        return ResponseEntity.ok(server.removeNode(nodeId));
    }

    //TODO rebalance from a set of consistent ids.
    @PutMapping("/rebalance/custom")
    public ResponseEntity<Object> customRebalance(@RequestHeader("password") String pass, @RequestBody RebalanceRequest request) {
        return ResponseEntity.ok(server.rebalance(request.getConsistendIds()));
    }

    @PutMapping("/rebalance")
    public ResponseEntity<Object> rebalance(@RequestHeader("password") String pass) {
        verify(pass);
        return ResponseEntity.ok(server.rebalance());
    }

    private void verify(String pass) {
        String hashed = digestPass(pass);
        if (!PASSWORD_HASHED.equals(hashed)) {
            throw new RuntimeException("Password is missing; Throw a custom exception;" +
                    " Implement an exception advisor; since password will be used every where.");
        }
    }

    private static String digestPass(String pass) {
        return DigestUtils.md5Hex(pass).toUpperCase();
    }


    public static void main(String[] args) {
        log.info("hashed password : {}", digestPass("something goes here"));
    }
}

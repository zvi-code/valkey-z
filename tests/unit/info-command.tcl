start_server {tags {"info and its relative command"}} {
    test "Extract version and sha1 details from info command and print" {
        set i [r info]
        regexp {redis_version:(.*?)\r\n} $i - version
        regexp {redis_git_sha1:(.*?)\r\n} $i - sha1
        puts "Testing Valkey version $version ($sha1)"
    }

    test "info command with at most one sub command" {
        foreach arg {"" "all" "default" "everything"} {
            if {$arg == ""} {
                set info [r 0 info]
            } else {
                set info [r 0 info $arg]
            }

            assert { [string match "*redis_version*" $info] }
            assert { [string match "*used_cpu_user*" $info] }
            assert { ![string match "*sentinel_tilt*" $info] }
            assert { [string match "*used_memory*" $info] }
            if {$arg == "" || $arg == "default"} {
                assert { ![string match "*rejected_calls*" $info] }        
            } else {
                assert { [string match "*rejected_calls*" $info] }        
            }        
        }
    }

    test "info command with one sub-section" {
        set info [r info cpu]
        assert { [string match "*used_cpu_user*" $info] }
        assert { ![string match "*sentinel_tilt*" $info] }
        assert { ![string match "*used_memory*" $info] }

        set info [r info sentinel]
        assert { ![string match "*sentinel_tilt*" $info] }
        assert { ![string match "*used_memory*" $info] }

        set info [r info commandSTATS] ;# test case insensitive compare
        assert { ![string match "*used_memory*" $info] }
        assert { [string match "*rejected_calls*" $info] }
    }

    test "info command with multiple sub-sections" {
        set info [r info cpu sentinel]
        assert { [string match "*used_cpu_user*" $info] }
        assert { ![string match "*sentinel_tilt*" $info] }
        assert { ![string match "*master_repl_offset*" $info] }

        set info [r info cpu all]
        assert { [string match "*used_cpu_user*" $info] }
        assert { ![string match "*sentinel_tilt*" $info] }
        assert { [string match "*used_memory*" $info] }
        assert { [string match "*master_repl_offset*" $info] }
        assert { [string match "*rejected_calls*" $info] }
        # check that we didn't get the same info twice
        assert { ![string match "*used_cpu_user_children*used_cpu_user_children*" $info] }

        set info [r info cpu default]
        assert { [string match "*used_cpu_user*" $info] }
        assert { ![string match "*sentinel_tilt*" $info] }
        assert { [string match "*used_memory*" $info] }
        assert { [string match "*master_repl_offset*" $info] }
        assert { ![string match "*rejected_calls*" $info] }
        # check that we didn't get the same info twice
        assert { ![string match "*used_cpu_user_children*used_cpu_user_children*" $info] }
    }
   
}

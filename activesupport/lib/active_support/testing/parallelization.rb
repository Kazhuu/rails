# frozen_string_literal: true

require "drb"
require "drb/unix" unless Gem.win_platform?
require "active_support/core_ext/module/attribute_accessors"

module ActiveSupport
  module Testing
    class Parallelization # :nodoc:
      class Server
        include DRb::DRbUndumped

        def initialize
          @queue = Queue.new
          @methods = []
          @last_klass = nil
          @last_reporter = nil
        end

        def record(reporter, result)
          raise DRb::DRbConnError if result.is_a?(DRb::DRbUnknown)

          reporter.synchronize do
            reporter.record(result)
          end
        end

        # Buffer test methods for one test class and push them to the queue as
        # a group.
        def <<(o)
          if o.nil?
            @queue << [@last_klass, @methods, @last_reporter] unless @methods.empty?
            @methods = []
            @queue << o
            return
          end
          @last_klass = o[0] if @last_klass.nil?
          if @last_klass != o[0]
            @queue << [@last_klass, @methods, @last_reporter]
            @last_klass = o[0]
            @methods = []
          end
          @last_reporter = DRbObject.new(o[2])
          @methods << o[1]
        end

        def pop; @queue.pop; end
      end

      @@after_fork_hooks = []

      def self.after_fork_hook(&blk)
        @@after_fork_hooks << blk
      end

      cattr_reader :after_fork_hooks

      @@run_cleanup_hooks = []

      def self.run_cleanup_hook(&blk)
        @@run_cleanup_hooks << blk
      end

      cattr_reader :run_cleanup_hooks

      def initialize(queue_size)
        @queue_size = queue_size
        @queue      = Server.new
        @pool       = []

        @url = DRb.start_service("drbunix:", @queue).uri
      end

      def after_fork(worker)
        self.class.after_fork_hooks.each do |cb|
          cb.call(worker)
        end
      end

      def run_cleanup(worker)
        self.class.run_cleanup_hooks.each do |cb|
          cb.call(worker)
        end
      end

      def start
        @pool = @queue_size.times.map do |worker|
          fork do
            DRb.stop_service

            after_fork(worker)

            queue = DRbObject.new_with_uri(@url)

            while job = queue.pop
              klass    = job[0]
              methods  = job[1]
              reporter = job[2]
              klass.with_info_handler reporter do
                methods.each do |method_name|
                  result = Minitest.run_one_method(klass, method_name)
                  begin
                    queue.record(reporter, result)
                  rescue DRb::DRbConnError
                    result.failures.each do |failure|
                      failure.exception = DRb::DRbRemoteError.new(failure.exception)
                    end
                    queue.record(reporter, result)
                  end
                end
              end
            end
          ensure
            run_cleanup(worker)
          end
        end
      end

      def <<(work)
        @queue << work
      end

      def shutdown
        @queue_size.times { @queue << nil }
        @pool.each { |pid| Process.waitpid pid }
      end
    end
  end
end

# frozen_string_literal: true

RSpec.describe Tonic::Rails::Client::RetryPolicy do
  describe ".from" do
    it "builds a policy from true, a Hash or a policy, and none from false or nil" do
      defaults = described_class.from(true)
      expect(defaults).to have_attributes(max_attempts: 3, codes: [14], initial_backoff: 0.1, max_backoff: 1,
                                          multiplier: 2)
      expect(described_class.from(max_attempts: 5, codes: %i[unavailable resource_exhausted]))
        .to have_attributes(max_attempts: 5, codes: [14, 8], initial_backoff: 0.1)
      expect(described_class.from(defaults)).to be(defaults)
      expect(described_class.from(false)).to be_nil
      expect(described_class.from(nil)).to be_nil
    end

    it "rejects anything else" do
      expect { described_class.from("yes") }.to raise_error(ArgumentError, /retries must be/)
      expect { described_class.from(attempts: 2) }.to raise_error(ArgumentError, /attempts/)
    end
  end

  it "validates its fields" do
    expect { described_class.from(max_attempts: 1) }.to raise_error(ArgumentError, /from 2 to 5/)
    expect { described_class.from(max_attempts: 6) }.to raise_error(ArgumentError, /from 2 to 5/)
    expect { described_class.from(codes: []) }.to raise_error(ArgumentError, /at least one/)
    expect { described_class.from(codes: [:ok]) }.to raise_error(ArgumentError, /other than OK/)
    expect { described_class.from(codes: [:nope]) }.to raise_error(ArgumentError, /unknown gRPC status code/)
    expect { described_class.from(initial_backoff: 0) }.to raise_error(ArgumentError, /initial_backoff/)
    expect { described_class.from(max_backoff: -1) }.to raise_error(ArgumentError, /max_backoff/)
    expect { described_class.from(multiplier: 0.5) }.to raise_error(ArgumentError, /multiplier/)
  end

  it "retries only the codes it names" do
    policy = described_class.from(codes: [:unavailable, 8])
    expect(policy.retry?(14)).to be(true)
    expect(policy.retry?(8)).to be(true)
    expect(policy.retry?(13)).to be(false)
  end

  it "waits a random time up to a cap that grows to max_backoff" do
    policy = described_class.from(initial_backoff: 0.1, multiplier: 2, max_backoff: 0.3)
    waits = Array.new(200) { [policy.backoff(1), policy.backoff(2), policy.backoff(3)] }.transpose
    expect(waits.map(&:max)).to all(be <= 0.3)
    expect(waits[0].max).to be <= 0.1
    expect(waits[1].max).to be_between(0.1, 0.2)
    expect(waits[2].max).to be > 0.2
    expect(waits[0].uniq.size).to be > 100
  end
end
